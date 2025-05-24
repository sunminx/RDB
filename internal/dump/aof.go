package dump

// The evolution history of aof.
// Since redis 2.x:
//  1. All write commands are appended to the AOF file in Text Protocol format (RESP, Redis Serialization Protocol).
//  2. By regularly rewriting the aof file, deleting and merging write commands, a compact new AOF file is generated.
//
// Since redis 4.x:
//  1. Import aof-use-rdb-preamble yes, When rewriting AOF, first store the database snapshot in RDB format,
//     and then append the incremental AOF command.
//
// Since redis 7.x:
//  1. The RDB part and the AOF part are no longer saved in one file.
//  2. The AOF file is split into multiple small files (similar to the segmentation of WAL logs)
//     to avoid a single AOF file being too large.
//  3. Introduce the manifest file to track the newly generated base file and incremental file.
//
// appendonlydir/
// ├── appendonly.aof.1.base.rdb   # basic data in RDB format
// ├── appendonly.aof.1.incr.aof   # incremental write commands
// ├── appendonly.aof.2.incr.aof
// └── appendonly.aof.manifest    # record sharding information

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"syscall"

	"github.com/sunminx/RDB/internal/cmd"
	"github.com/sunminx/RDB/internal/db"
	"github.com/sunminx/RDB/internal/hash"
	"github.com/sunminx/RDB/internal/list"
	"github.com/sunminx/RDB/internal/networking"
	obj "github.com/sunminx/RDB/internal/object"
	"github.com/sunminx/RDB/internal/rio"
	"github.com/sunminx/RDB/internal/sds"
	"github.com/sunminx/RDB/pkg/util"
)

type Aofer struct {
	file    *os.File // The currently open file
	rd      *rio.Reader
	wr      *rio.Writer
	cksum   int64
	db      *db.DB
	fakeCli *networking.Client
}

func newAofer(db *db.DB) *Aofer {
	return &Aofer{
		db:      db,
		fakeCli: networking.NewClient(nil, db),
	}
}

func (aof *Aofer) setFile(file *os.File, mode byte) error {
	if mode == 'r' {
		rd, err := rio.NewReader(file)
		if err != nil {
			return errors.Join(err, errors.New("failed new rio.Reader"))
		}
		aof.rd = rd
	} else if mode == 'w' {
		wr, err := rio.NewWriter(file)
		if err != nil {
			return errors.Join(err, errors.New("failed new rio.Writer"))
		}
		aof.wr = wr
	} else {
		return errors.New("invalid mode")
	}
	aof.file = file
	return nil
}

func (aof *Aofer) closeFile() {
	if aof.file != nil {
		aof.file.Close()
	}
}

func (aof *Aofer) rewrite(timestamp int64) error {
	var err error
	for e := range aof.db.Iterator() {
		switch e.Val.Type() {
		case obj.TypeString:
			if !aof.rewriteStringObject(e.Key, e.Val) {
				return errors.New("failed rewrite string object, key = " + e.Key)
			}
		case obj.TypeList:
			if !aof.rewriteListObject(e.Key, e.Val) {
				return errors.New("failed rewrite list object, key = " + e.Key)
			}
		case obj.TypeHash:
			if !aof.rewriteHashObject(e.Key, e.Val) {
				return errors.New("failed rewrite hash object, key = " + e.Key)
			}
		default:
			return errors.New("invalid type of robj in AOF file")
		}

		expire := aof.db.Expire(e.Key)
		if expire != -1 {
			cmd := "*3\r\n$9\r\nPEXPIREAT\r\n"
			if _, err = aof.wr.Write([]byte(cmd)); err != nil {
				return errors.Join(err, errors.New("failed rewrite expire for key "+e.Key))
			}
			if !aof.writeBulkString([]byte(e.Key)) {
				return errors.Join(err, errors.New("failed rewrite expire for key "+e.Key))
			}
			if !aof.writeBulkInt(int64(expire)) {
				return errors.Join(err, errors.New("failed rewrite expire for key "+e.Key))
			}
		}
	}
	return nil
}

func (aof *Aofer) rewriteStringObject(key string, val *obj.Robj) bool {
	cmd := "*3\r\n$3\r\nSET\r\n"
	if _, err := aof.wr.Write([]byte(cmd)); err != nil {
		slog.Warn("failed rewrite string object cmd", "err", err)
		return noRewrite
	}
	if !aof.writeBulkString([]byte(key)) {
		return noRewrite
	}
	if !aof.writeBulkObject(val) {
		return noRewrite
	}
	return rewrited
}

const aofRewriteItemsPerCmd = 64

func (aof *Aofer) rewriteListObject(key string, val *obj.Robj) bool {
	batch, entries := 0, list.Cnt(val)
	iter := list.NewIterator(val)
	for iter.HasNext() {
		if batch == 0 {
			cmdEntries := util.Cond(entries < aofRewriteItemsPerCmd,
				entries, aofRewriteItemsPerCmd)
			if !aof.writeMultibulkCount(2+cmdEntries) ||
				!aof.writeBulkString([]byte("RPUSH")) ||
				!aof.writeBulkString([]byte(key)) {
				return noRewrite
			}
		}

		entry := iter.Next()
		if !aof.writeBulkString(entry.([]byte)) {
			return noRewrite
		}
		entries--
		batch++
		if batch == aofRewriteItemsPerCmd {
			batch = 0
		}
	}
	return rewrited
}

func (aof *Aofer) rewriteHashObject(key string, val *obj.Robj) bool {
	batch, entries := 0, hash.Len(val)
	iter := hash.NewIterator(val)
	for iter.HasNext() {
		if batch == 0 {
			cmdEntries := util.Cond(entries < aofRewriteItemsPerCmd,
				entries, aofRewriteItemsPerCmd)
			if !aof.writeMultibulkCount(2+cmdEntries*2) ||
				!aof.writeBulkString([]byte("HMSET")) ||
				!aof.writeBulkString([]byte(key)) {
				return noRewrite
			}
		}

		kvPair := iter.Next().(hash.KVPair)
		if !aof.writeBulkString(kvPair[0]) {
			return noRewrite
		}
		if !aof.writeBulkString(kvPair[1]) {
			return noRewrite
		}
		entries--
		batch++
		if batch == aofRewriteItemsPerCmd {
			batch = 0
		}
	}
	return rewrited
}

func (aof *Aofer) writeBulkObject(robj *obj.Robj) bool {
	if robj.CheckEncoding(obj.EncodingInt) {
		return aof.writeBulkInt(robj.Val().(int64))
	} else if robj.CheckEncoding(obj.EncodingRaw) {
		return aof.writeBulkString([]byte(robj.Val().(sds.SDS)))
	}
	slog.Warn("unknown string encoding object")
	return noRewrite
}

func (aof *Aofer) writeBulkInt(n int64) bool {
	bytes := util.Int64ToBytes(n)
	aof.wr.Write([]byte{'$'})
	aof.wr.Write(bytes)
	aof.wr.Write([]byte("\r\n"))
	return true
}

func (aof *Aofer) writeBulkString(s []byte) bool {
	ln := int64(len(s))
	if !aof.writeBulkInt(ln) {
		slog.Warn("failed write bulk string in rewrite aof")
		return noRewrite
	}
	if ln > 0 {
		if _, err := aof.wr.Write(s); err != nil {
			slog.Warn("failed write bulk string in rewrite aof")
			return noRewrite
		}
	}
	if _, err := aof.wr.Write([]byte("\r\n")); err != nil {
		slog.Warn("failed write bulk string in rewrite aof", "err", err)
		return noRewrite
	}
	return rewrited
}

func (aof *Aofer) writeMultibulkCount(c int64) bool {
	var err error
	if _, err = aof.wr.Write([]byte{'*'}); err != nil {
		slog.Warn("failed write multibulk count", "err", err)
		return noRewrite
	}
	if _, err = aof.wr.Write(util.Int64ToBytes(c)); err != nil {
		slog.Warn("failed write multibulk count", "err", err)
		return noRewrite
	}
	if _, err = aof.wr.Write([]byte("\r\n")); err != nil {
		slog.Warn("failed write multibulk count", "err", err)
		return noRewrite
	}
	return rewrited
}

type aofFileType byte

const (
	none = '0'
	base = 'b'
	hist = 'h'
	incr = 'i'
)

const (
	aofOk        = 0
	aofNotExist  = 1
	aofEmpty     = 2
	aofOpenErr   = 3
	aofFailed    = 4
	aofTruncated = 5
)

func (aof *Aofer) aofFileSize() int64 {
	info, err := aof.file.Stat()
	if err != nil {
		return -1
	}
	return info.Size()
}

func (aof *Aofer) loadSingleFile(filename string, server *networking.Server) int {
	var (
		validUpTo              int64
		validBeforeMulti       int64
		lastProgressReportSize int64
		loops                  int64
		ret                    int
	)

	// Check if the AOF file is in RDB format (it may be RDB encoded base AOF
	// or old style RDB-preamble AOF). In that case we need to load the RDB file
	// and later continue loading the AOF tail if it is an old style RDB-preamble AOF.
	preambleMode := true
	p, err := aof.readRaw(5)
	if err != nil || string(p) != "REDIS" {
		preambleMode = false
	}
	if err = aof.rd.Reset(); err != nil {
		slog.Warn("failed seek to the starting position of the AOF file", "filename", filename)
		return aofFailed
	}

	if preambleMode {
		// Since redis 4.x aof-use-rdb-preamble
		if server.AofFilename == filename {
			slog.Info("reading RDB preamble from AOF file...")
		} else {
			// Since redis 7.x aof-chunking
			slog.Info("reading RDB base file on AOF loading...")
		}
		rdber, err := newRdbSaver(aof.file, 'r', server.DB, newRdberInfo(server))
		if err != nil {
			slog.Warn("failed create rdber before loading", "filename", filename, "err", err)
			return aofFailed
		}
		// Laoding RDB part firstly.
		if err = rdber.load(); err != nil {
			if server.AofFilename == filename {
				slog.Info("failed reading RDB preamble from AOF file...", "err", err)
			} else {
				slog.Info("failed reading RDB base file on AOF loading...", "err", err)
			}
			return aofFailed
		} else {
			pos := aof.rd.Tell()
			// During the rdb loading stage, the progress is reported only once.
			loadingAbsProgress(server, pos)
			lastProgressReportSize = pos
			if server.AofFilename == filename {
				slog.Info("reading the remaining AOF tail...")
			}
		}
	}

	for {
		loops++
		// During the aof loading phase, the progress is reported every 1024 times.
		if loops%1024 == 0 {
			processDelta := aof.rd.Tell() - lastProgressReportSize
			loadingAbsProgress(server, processDelta)
			lastProgressReportSize += processDelta
		}

		p, isPrefix, err := aof.rd.ReadLine()
		if err != nil || isPrefix {
			if err == io.EOF {
				break
			}
			slog.Warn("unrecoverable error reading the append only file", "filename", filename)
			return aofFailed
		}
		if p[0] == '#' {
			continue
		}
		if p[0] != '*' {
			slog.Warn("invalid protocol, not found *")
			return aofFailed
		}
		argc, err := strconv.ParseInt(string(p[1:]), 10, 64)
		if err != nil || argc < 1 {
			slog.Warn("invalid protocol, argc is less than 1")
			return aofFailed
		}

		argv := make([][]byte, argc, argc)
		for i := int64(0); i < argc; i++ {
			p, isPrefix, err := aof.rd.ReadLine()
			if isPrefix || err != nil {
				slog.Warn("unrecoverable error reading the append only file", "filename", filename)
				return aofFailed
			}
			if p[0] != '$' {
				slog.Warn("Bad file format reading the append only file make a backup "+
					"of your AOF file, then use ./redis-check-aof --fix <filename.manifest>",
					"filename", filename)
				return aofFailed
			}
			ln, err := strconv.ParseInt(string(p[1:]), 10, 64)
			if err != nil || ln < 1 {
				slog.Warn("invalid protocol, invalid bulk len")
				return aofFailed
			}
			p, err = aof.readRaw(int(ln))
			if err != nil {
				slog.Warn("failed read bulk")
				return aofFailed
			}

			argv[i] = p

			// Discard "CRLF"
			_, err = aof.readRaw(2)
			if err != nil {
				slog.Warn(`failed discard \r\n`)
				return aofFailed
			}
		}

		aof.fakeCli.SetArgument(argv)
		name := strings.ToLower(string(argv[0]))
		command, err := aof.lookupCommand(name, int(argc))
		if err != nil {
			slog.Warn("failed exec command when loading AOF file", "err", err)
			return aofFailed
		}
		if server.AofLoadTruncated {
			validBeforeMulti = validUpTo
		}
		if aof.fakeCli.Multi() && command.Name != "exec" {
			aof.fakeCli.QueueMultiCommand()
		} else {
			command.Proc(aof.fakeCli)
		}
		if server.AofLoadTruncated {
			validUpTo = aof.rd.Tell()
		}
	}

	if aof.fakeCli.Multi() {
		validUpTo = validBeforeMulti
		if server.AofLoadTruncated {
			if validUpTo == -1 {
				slog.Warn("last valid command offset is invalid", "filename", filename)
			} else {
				if aof.rd.Truncate(validUpTo) != nil {
					slog.Warn("truncate aof file failed")
				} else {
					// Reset offset which we had loaded.
					if _, err := aof.rd.Seek(0, 2); err != nil {
						slog.Warn("can't seek the end of the AOF file", "filename", filename)
					} else {
						slog.Warn("AOF loaded anyway because aof-load-truncated is enabled", "filename", filename)
						loadingAbsProgress(server, aof.rd.Tell()-lastProgressReportSize)
						return aofTruncated
					}
				}
			}
		}
		slog.Warn("Unexpected end of file reading the append only file . You can: " +
			"1) Make a backup of your AOF file, then use ./redis-check-aof --fix <filename.manifest>. " +
			"2) Alternatively you can set the 'aof-load-truncated' configuration option to yes and restart the server.")
		return aofFailed
	}

	// Update the amount of data loaded in the last round.
	loadingAbsProgress(server, aof.rd.Tell()-lastProgressReportSize)
	return ret
}

func (aof *Aofer) lookupCommand(name string, argc int) (cmd.Command, error) {
	command, found := aof.fakeCli.Server.LookupCommand(name)
	if found {
		if (command.Arity > 0 && command.Arity != argc) || (argc < -command.Arity) {
			return command, fmt.Errorf("wrong number of arguments for %q command", name)
		}
	}
	if !found {
		return command, errors.New("command " + name + " not found")
	}
	return command, nil
}

func (aof *Aofer) readRaw(n int) ([]byte, error) {
	p := make([]byte, n, n)
	if _, err := aof.rd.Read(p); err != nil {
		return nil, err
	}
	return p, nil
}

type aofManifest struct {
	baseAofInfo     *aofInfo
	incrAofInfos    []*aofInfo
	histAofInfos    []*aofInfo
	currBaseFileSeq int64
	currIncrFileSeq int64
	dirty           bool
}

func newAofManifest() *aofManifest {
	return &aofManifest{
		incrAofInfos: make([]*aofInfo, 0),
		histAofInfos: make([]*aofInfo, 0),
	}
}

type aofInfo struct {
	name        string
	typ         aofFileType
	seq         int64
	startOffset int64
	endOffset   int64
}

func newAofInfo() *aofInfo {
	return &aofInfo{
		typ:         none,
		seq:         -1,
		startOffset: -1,
		endOffset:   -1,
	}
}

func (ai *aofInfo) format() []byte {
	buf := make([]byte, 0)
	buf = append(buf, []byte(aofManifestKeyFileName)...)
	buf = append(buf, []byte(" ")...)
	buf = append(buf, []byte(ai.name)...)
	buf = append(buf, []byte(" ")...)
	buf = append(buf, []byte(aofManifestKeyFileSeq)...)
	buf = append(buf, []byte(" ")...)
	buf = append(buf, []byte(strconv.FormatInt(ai.seq, 10))...)
	buf = append(buf, []byte(" ")...)
	buf = append(buf, []byte(aofManifestKeyFileType)...)
	buf = append(buf, []byte(" ")...)
	buf = append(buf, byte(ai.typ))
	buf = append(buf, []byte(" ")...)
	buf = append(buf, []byte(aofManifestKeyFileStartoffset)...)
	buf = append(buf, []byte(" ")...)
	buf = append(buf, []byte(strconv.FormatInt(ai.startOffset, 10))...)
	buf = append(buf, []byte(" ")...)
	buf = append(buf, []byte(aofManifestKeyFileEndoffset)...)
	buf = append(buf, []byte(" ")...)
	buf = append(buf, []byte(strconv.FormatInt(ai.endOffset, 10))...)
	buf = append(buf, []byte("\n")...)
	return buf
}

const manifestMaxLine = 1024

// AOF manifest key.
const (
	aofManifestKeyFileName        = "file"
	aofManifestKeyFileSeq         = "seq"
	aofManifestKeyFileType        = "type"
	aofManifestKeyFileStartoffset = "startoffset"
	aofManifestKeyFileEndoffset   = "endoffset"
)

var errManifestFileNotFound = errors.New("manifest file not found")

func createAofManifest(file *os.File) (*aofManifest, error) {
	am := newAofManifest()
	rd := bufio.NewReader(file)
	for {
		line, isPrefix, err := rd.ReadLine()
		if err != nil {
			if err == io.EOF {
				slog.Info("read AOF manifest file finished")
				break
			}
			return nil, errors.Join(err, errors.New("failed read AOF manifest file"))
		}

		if line[0] == '#' {
			continue
		}
		if isPrefix || len(line) >= manifestMaxLine {
			return nil, errors.New("the AOF manifest file contains too long line")
		}

		argv := strings.Split(string(line), " ")
		argc := len(argv)
		if argc < 6 || argc%2 != 0 {
			return nil, errors.New("invalid AOF manifest file format")
		}

		ai := newAofInfo()

		var maxSeq int64
		for i := 0; i < argc; i += 2 {
			switch argv[i] {
			case aofManifestKeyFileName:
				name := argv[i+1]
				if strings.Index(name, "/") != -1 {
					return nil, errors.New("file can't be a path, just a filename")
				}
				ai.name = name
			case aofManifestKeyFileSeq:
				seq, err := strconv.ParseInt(argv[i+1], 10, 64)
				if err != nil {
					return nil, errors.Join(err, errors.New("invalid seq value"))
				}
				ai.seq = seq
			case aofManifestKeyFileType:
				ai.typ = aofFileType(argv[i+1][0])
			case aofManifestKeyFileStartoffset:
				start, err := strconv.ParseInt(argv[i+1], 10, 64)
				if err != nil {
					return nil, errors.Join(err, errors.New("invalid startOffset value"))
				}
				ai.startOffset = start
			case aofManifestKeyFileEndoffset:
				end, err := strconv.ParseInt(argv[i+1], 10, 64)
				if err != nil {
					return nil, errors.Join(err, errors.New("invalid endOffset value"))
				}
				ai.endOffset = end
			default:
			}
		}

		if ai.name == "" || ai.seq == -1 || ai.typ == '0' {
			return nil, errors.New("invalid AOF manifest format")
		}

		switch ai.typ {
		case base:
			am.baseAofInfo = ai
			am.currBaseFileSeq = ai.seq
		case hist:
			am.histAofInfos = append(am.histAofInfos, ai)
		case incr:
			if ai.seq <= maxSeq {
				return nil, errors.New("found a non-monotonic sequence number")
			}
			am.incrAofInfos = append(am.incrAofInfos, ai)
			am.currIncrFileSeq = ai.seq
			maxSeq = ai.seq
		default:
			return nil, errors.New("unknown AOF manifest type")
		}
	}
	return am, nil
}

func (am *aofManifest) fileNum() int {
	fileNum := len(am.incrAofInfos)
	if am.baseAofInfo != nil {
		fileNum++
	}
	return fileNum
}

const (
	baseFileSuffix     = ".base"
	incrFileSuffix     = ".incr"
	rdbFormatSuffix    = ".rdb"
	aofFormatSuffix    = ".aof"
	manifestNameSuffix = ".manifest"
	tempFileNamePrefix = "temp-"
)

func (am *aofManifest) nextBaseAofName(server *networking.Server) string {
	if am.baseAofInfo != nil {
		am.baseAofInfo.typ = hist
		am.histAofInfos = append(am.histAofInfos, am.baseAofInfo)
	}
	ai := newAofInfo()
	formatSuffix := util.Cond(server.AofUseRdbPreamble, rdbFormatSuffix, aofFormatSuffix)
	am.currBaseFileSeq++
	ai.name = fmt.Sprintf("%s.%d%s%s", server.AofFilename, am.currBaseFileSeq,
		baseFileSuffix, formatSuffix)
	ai.typ = base
	ai.seq = am.currBaseFileSeq
	am.baseAofInfo = ai
	return ai.name

}

func (am *aofManifest) nextIncrAofName(server *networking.Server) string {
	ai := newAofInfo()
	ai.typ = incr
	am.currIncrFileSeq++
	ai.name = fmt.Sprintf("%s.%d%s%s", server.AofFilename, am.currIncrFileSeq,
		incrFileSuffix, aofFormatSuffix)
	ai.seq = am.currIncrFileSeq
	am.incrAofInfos = append(am.incrAofInfos, ai)
	return ai.name
}

func (am *aofManifest) moveIncrAofToHist() {
	ln := len(am.incrAofInfos)
	if ln < 2 {
		return
	}
	am.histAofInfos = append(am.histAofInfos, am.incrAofInfos[:ln-1]...)
	am.incrAofInfos = am.incrAofInfos[ln-1:]
}

func (am *aofManifest) persist(server *networking.Server) error {
	tempManifestFilename := TempAofManifestFilename(server.AofFilename)
	tempManifestFilepath := makePath(server.AofDirname, tempManifestFilename)
	file, err := os.Create(tempManifestFilepath)
	if err != nil {
		return errors.New("failed create temp AOF manifest file")
	}

	defer func() {
		if file != nil {
			file.Close()
			os.Remove(tempManifestFilepath)
		}
	}()

	var buf []byte
	if am.baseAofInfo != nil {
		buf = am.baseAofInfo.format()
		if err := write(file, buf); err != nil {
			return errors.Join(err, errors.New("failed write AOF base manifest"))
		}
	}

	for _, ai := range am.histAofInfos {
		buf = ai.format()
		if err := write(file, buf); err != nil {
			return errors.Join(err, errors.New("failed write AOF hist manifest"))
		}
	}

	for _, ai := range am.incrAofInfos {
		buf = ai.format()
		if err := write(file, buf); err != nil {
			return errors.Join(err, errors.New("failed write AOF incr manifest"))
		}
	}

	manifestFilename := aofManifestFilename(server.AofFilename)
	manifestFilenpath := makePath(server.AofDirname, manifestFilename)
	if err := os.Rename(tempManifestFilepath, manifestFilenpath); err != nil {
		return err
	}
	return nil
}

func (am *aofManifest) deleteAofHistFiles(server *networking.Server) {
	for _, ai := range am.histAofInfos {
		filepath := makePath(server.AofDirname, ai.name)
		if err := os.Remove(filepath); err != nil {
			slog.Warn("failed delete AOF hist file", "filepath", filepath, "err", err)
		}
	}
}

func write(file *os.File, p []byte) error {
	total := len(p)
	for total > 0 {
		n, err := file.Write(p)
		if err != nil {
			if err == syscall.EINTR {
				continue
			}
			return err
		}
		total -= n
	}
	return nil
}

func aofManifestFilename(aof string) string {
	return aof + manifestNameSuffix
}

func TempAofManifestFilename(aof string) string {
	return tempFileNamePrefix + aof + manifestNameSuffix
}

func tempIncrAofName(aof string) string {
	return tempFileNamePrefix + aof + incrFileSuffix
}

func makePath(path, filename string) string {
	if path == "" {
		path = "."
	}
	return path + "/" + filename
}
