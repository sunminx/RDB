package datastruct

import (
	"encoding/binary"
	"math"
	"strconv"

	"github.com/sunminx/RDB/pkg/util"
)

// <zlbytes> <zltail> <zllen> <entry> <entry> ... <entry> <zlend>
// uint32 	 uint32   uint16
//
// <prevlen> <encoding> <entry-data>
// <prevlen from 0 to 253> <encoding> <entry>
// 0xFE <4 bytes unsigned little endian prevlen> <encoding> <entry>

// <encoding>
// <encoding><lensize><len>
type Ziplist []byte

const (
	ZiplistHeaderSize = 4*2 + 2
	ZiplistEndSize    = 1

	ZiplistEnd = 255
)

func NewZiplist() *Ziplist {
	bytes := int32(ZiplistHeaderSize + ZiplistEndSize)

	zl := Ziplist(make([]byte, bytes, bytes))
	zl.SetZlbytes(bytes)
	zl.SetZltail(ZiplistHeaderSize)
	zl[bytes-1] = ZiplistEnd
	return &zl
}

func (zl *Ziplist) SetZlbytes(bytes int32) {
	binary.LittleEndian.PutUint32([]byte(*zl)[:4], uint32(bytes))
}

func (zl *Ziplist) AddZlbytes(bytes int32) {
	bytes += zl.Zlbytes()
	binary.LittleEndian.PutUint32([]byte(*zl)[:4], uint32(bytes))
}

func (zl *Ziplist) Zlbytes() int32 {
	return int32(binary.LittleEndian.Uint32([]byte(*zl)[:4]))
}

func (zl *Ziplist) SetZltail(tail int32) {
	binary.LittleEndian.PutUint32([]byte(*zl)[4:8], uint32(tail))
}

func (zl *Ziplist) AddZltail(tail int32) {
	tail += zl.Zltail()
	binary.LittleEndian.PutUint32([]byte(*zl)[4:8], uint32(tail))
}

func (zl *Ziplist) Zlhead() int32 {
	return ZiplistHeaderSize
}

func (zl *Ziplist) Zltail() int32 {
	return int32(binary.LittleEndian.Uint32([]byte(*zl)[4:8]))
}

func (zl *Ziplist) SetZllen(ln int16) {
	binary.LittleEndian.PutUint16([]byte(*zl)[8:10], uint16(ln))
}

func (zl *Ziplist) AddZllen(ln int16) {
	ln += zl.Zllen()
	binary.LittleEndian.PutUint16([]byte(*zl)[8:10], uint16(ln))
}

func (zl *Ziplist) Zllen() int16 {
	return int16(binary.LittleEndian.Uint16([]byte(*zl)[8:10]))
}

// the first two bits encode the type of the entry
// string:
//	   00xxxxxx
//	   01xxxxxx
//	   10xxxxxx
// int:
//     11xxxxxx

const (
	zipStrMask = 0xc0
	zipIntMask = 0x30

	zipStr06b = (0 << 6)
	zipStr14b = (1 << 6)
	zipStr32b = (2 << 6)

	zipInt16b = (0xc0 | 0<<4)
	zipInt32b = (0xc0 | 1<<4)
	zipInt64b = (0xc0 | 2<<4)
	zipInt24b = (0xc0 | 3<<4)
	zipInt8b  = 0xfe
)

// decodeEntry decode a entry locate by offset
func (zl *Ziplist) DecodeEntry(offset int32) (entry []byte, entrysize int32) {
	prevlensize := zl.prevLenSize(offset)
	_type, lensize, ln := zl.decodeEntryEncoding(offset + prevlensize)
	conoffset := offset + prevlensize + lensize
	if _type == strType {
		entry = []byte(*zl)[conoffset : conoffset+ln]
	} else {
		if ln == 0 {
			encoding := []byte(*zl)[offset+prevlensize : offset+prevlensize+1]
			num := (encoding[0] & 0x0F) - 1
			entry = []byte(strconv.FormatInt(int64(num), 10))
		} else if ln == 1 {
			num := []byte(*zl)[conoffset : conoffset+ln]
			entry = []byte(strconv.FormatInt(int64(num[0]), 10))
		} else if ln == 2 {
			num := binary.LittleEndian.Uint16([]byte(*zl)[conoffset : conoffset+ln])
			entry = []byte(strconv.FormatInt(int64(num), 10))
		} else if ln == 4 {
			num := binary.LittleEndian.Uint32([]byte(*zl)[conoffset : conoffset+ln])
			entry = []byte(strconv.FormatInt(int64(num), 10))
		} else if ln == 8 {
			num := binary.LittleEndian.Uint64([]byte(*zl)[conoffset : conoffset+ln])
			entry = []byte(strconv.FormatInt(int64(num), 10))
		}
	}
	entrysize = prevlensize + lensize + ln
	return
}

const (
	strType int8 = 0
	intType int8 = 1
)

func (zl *Ziplist) decodeEntryEncoding(offset int32) (strorint int8,
	lensize, ln int32) {
	// _type: first two bit of byte
	_type := []byte(*zl)[offset] & zipStrMask
	encoding := []byte(*zl)[offset : offset+4]
	if _type < zipStrMask { // string
		strorint = strType
		lensize, ln = zipStrSize(_type, encoding)
	} else { // int
		strorint = intType
		lensize, ln = zipIntSize([]byte(*zl)[offset], encoding)
	}
	return
}

func zipStrSize(_type byte, encoding []byte) (int32, int32) {
	switch _type {
	case zipStr06b:
		// 0x3f 00111111
		// |00pppppp| - 1 byte
		// The last 6 bits of "encoding" encode the length of the string.
		return 1, int32(encoding[0]) & 0x3f
	case zipStr14b:
		// |01pppppp|qqqqqqqq| - 2 bytes
		// The last 14 bits of "encoding" encode the length of the string.
		return 2, (int32(encoding[0])&0x3f)<<8 | int32(encoding[1])
	case zipStr32b:
		// |10000000|qqqqqqqq|rrrrrrrr|ssssssss|tttttttt| - 5 bytes
		// The last 32 bits of "encoding" encode the length of the string.
		// The 6 lower bits of the first byte are not used and are set to zero.
		return 5, int32(encoding[1])<<24 | int32(encoding[2])<<16 | int32(encoding[3])<<8 | int32(encoding[4])
	default:
	}
	return 0, 0
}

func zipIntSize(_type byte, _ []byte) (int32, int32) {
	switch _type {
	case zipInt8b:
		return 1, 1
	case zipInt16b:
		return 1, 2
	case zipInt32b:
		return 1, 4
	case zipInt64b:
		return 1, 8
	default:
		if _type&0xf0 == 0xf0 {
			return 1, 0
		}
	}
	return 0, 0
}

func (zl *Ziplist) EncodeEntry(prevlen int32, content []byte) []byte {
	entry := make([]byte, 0)
	entry = append(entry, encodePrevLen(prevlen)...)
	_type, _, encoded := zl.encodeEntryEncoding(content)
	entry = append(entry, encoded...)
	if _type == strType {
		entry = append(entry, content...)
	}
	return entry
}

func (zl *Ziplist) encodeEntryEncoding(entry []byte) (int8, int32, []byte) {
	if len(entry) < 32 {
		if num, err := strconv.ParseInt(string(entry), 10, 32); err == nil {
			// <encoding-num-len>
			lensize, encoded := zipIntEncoding(int32(num))
			return intType, lensize, encoded
		}
	}
	lensize, encoded := zipStrEncoding(entry)
	return strType, lensize, encoded
}

// ZiplistEntryEncodeLen estimate the number of bytes occupied by the entry.
func ZiplistEntryEncodeLen(ln int32) int32 {
	var overhead int32

	if ln < 254 {
		overhead = 1
	} else {
		overhead = 5
	}

	if ln < 64 {
		overhead += 1
	} else if ln < 16384 {
		overhead += 2
	} else {
		overhead += 5
	}

	return overhead + ln
}

func zipIntEncoding(num int32) (int32, []byte) {
	var buf []byte
	switch {
	case num >= 0 && num < 12: // |1111xxxx| - (with xxxx between 0000 and 1101)
		return 1, []byte{byte(0xf1 + num)}
	case num >= math.MinInt8 && num <= math.MaxInt8: // |11111110| - 2 bytes
		return 2, []byte{zipInt8b, byte(num)}
	case num >= math.MinInt16 && num <= math.MaxInt16: // |11000000| - 3 bytes
		buf = make([]byte, 3)
		buf[0] = zipInt16b
		binary.LittleEndian.PutUint16(buf[1:], uint16(num))
		return 3, buf
	case num >= math.MinInt32 && num <= math.MaxInt32: // |11010000| - 5 bytes
		buf = make([]byte, 5)
		buf[0] = zipInt32b
		binary.LittleEndian.PutUint32(buf[1:], uint32(num))
		return 5, buf
	default:
	}
	return 0, buf
}

func zipStrEncoding(entry []byte) (int32, []byte) {
	ln := len(entry)
	switch {
	case ln <= 0x3f:
		// 0x3f 0011 1111
		// |00pppppp| - 1 byte
		return 1, []byte{byte(zipStr06b + ln)}
	case ln <= 0x3fff:
		// 0x3fff 0011 1111 1111 1111
		// |01pppppp|qqqqqqqq| - 2 bytes
		return 2, []byte{byte(zipStr14b | (ln>>8)&0x3f), byte(ln & 0xff)}
	default:
	}
	// |10000000|qqqqqqqq|rrrrrrrr|ssssssss|tttttttt| - 5 bytes
	return 5, []byte{zipStr32b, byte((ln >> 24) & 0xff),
		byte((ln >> 16) & 0xff), byte((ln >> 8) & 0xff), byte(ln & 0xff)}
}

func (zl *Ziplist) tailEntry() []byte {
	tailOffset := zl.Zltail()
	return []byte(*zl)[tailOffset:]
}

func (zl *Ziplist) entryLen(offset int32) int32 {
	prevlensize := prevLenSize(offset)
	_, lensize, ln := zl.decodeEntryEncoding(offset + prevlensize)
	return prevlensize + lensize + ln
}

func (zl *Ziplist) prevLenSize(offset int32) int32 {
	return prevLenSize(zl.PrevLen(offset))
}

func prevLenSize(prevlen int32) int32 {
	var prevlensize int32 = 1
	if prevlen >= 254 {
		prevlensize += 4
	}
	return prevlensize
}

func encodePrevLen(size int32) []byte {
	if size < 254 {
		return []byte{byte(size)}
	}
	buf := make([]byte, 5, 5)
	buf[0] = 0xFE
	binary.LittleEndian.PutUint32(buf[1:], uint32(size))
	return buf
}

func (zl *Ziplist) PrevLen(offset int32) int32 {
	if []byte(*zl)[offset] != 0xFE {
		return int32([]byte(*zl)[offset])
	}
	return int32(binary.LittleEndian.Uint32([]byte(*zl)[offset+1 : offset+5]))
}

func (zl *Ziplist) ReplaceAtIndex(index int16, entry []byte) {
	if index < 0 {
		index = 0
	}
	if index >= zl.Zllen() {
		index = zl.Zllen() - 1
	}
	offset := zl.offsetHeadSkipN(index)
	prevlen := zl.PrevLen(offset)
	encoded := zl.EncodeEntry(prevlen, entry)
	encodedlen := int32(len(encoded))
	oldencodedlen := zl.entryLen(offset)
	if encodedlen > oldencodedlen {
		zl.expand(offset, encodedlen-oldencodedlen)
	} else if encodedlen < oldencodedlen {
		zl.shrink(offset+(encodedlen-oldencodedlen), offset)
	}
	zl.write(offset, encoded)
	return
}

func (zl *Ziplist) Push(entry []byte) {
	offset := zl.Zlbytes() - 1
	zl.insert(offset, entry)
	return
}

func (zl *Ziplist) PushLeft(entry []byte) {
	offset := ZiplistHeaderSize
	zl.insert(int32(offset), entry)
	return
}

func (zl *Ziplist) insert(offset int32, content []byte) {
	var prevlen, nextdiff int32
	zlbytes := zl.Zlbytes()
	zllen := zl.Zllen()
	zltail := zl.Zltail()

	firstByte := ([]byte)(*zl)[offset]
	if firstByte == ZiplistEnd && zl.Zllen() > 0 { // end
		prevlen = zl.entryLen(zltail)
		zl.SetZltail(offset)
	}
	// calculate the number of bytes required to insert the entry
	// 1. prevlen
	// 2. encoding
	// 3. len(entry)

	// does the insert cause changes in the prevlen of next entry
	// +4 or not

	entry := zl.EncodeEntry(prevlen, content)
	entrysize := int32(len(entry))
	if firstByte != ZiplistEnd && zl.Zllen() > 0 { // header
		if entrysize >= 254 {
			nextdiff = 4
		}
		zl.SetZltail(offset + entrysize)
	}
	zl.expand(offset, entrysize+nextdiff)
	// store <prevlen><encoding><data> for entry
	zl.write(offset, entry)
	// reset prevlen for next entry
	if nextdiff > 0 {
		zl.write(offset+entrysize, encodePrevLen(entrysize))
	} else if firstByte != ZiplistEnd && zl.Zllen() == 0 {
		zl.write(offset+entrysize, []byte{byte(entrysize)})
	}
	zlbytes += entrysize + nextdiff
	zl.write(zlbytes-1, []byte{ZiplistEnd})
	zl.SetZllen(zllen + 1)
	zl.SetZlbytes(zlbytes)
	return
}

func (zl *Ziplist) InsertEncoded(offset int32, encoded []byte,
	ln int16, headprevlen, taillen int32) {

	encodedlen := int32(len(encoded))
	prevlen := zl.PrevLen(offset)

	var nextdiff int32
	// 头部插入 更新原来第一个entry的prevlen
	// 尾部插入 更新插入的encoded的第一个entry的prevlen
	if !zl.atEnd(offset) {
		if prevlen < 254 && taillen >= 254 {
			nextdiff = 4
		} else if prevlen >= 254 && taillen < 254 {
			nextdiff = -4
		}
		zl.expand(offset, encodedlen+nextdiff)
		zl.write(offset, encoded)
		zl.write(offset+encodedlen, encodePrevLen(taillen))
	} else {
		if prevlen < 254 && headprevlen >= 254 {
			nextdiff = 4
		} else if prevlen >= 254 && headprevlen < 254 {
			nextdiff = -4
		}
		zl.expand(offset, encodedlen+nextdiff)
		zl.write(offset, encoded)
		zl.write(offset, encodePrevLen(prevlen))
	}

	zl.AddZlbytes(encodedlen + nextdiff)
	zl.AddZllen(ln)
	zl.AddZltail(encodedlen + nextdiff)
	zl.write(zl.Zlbytes()-1, []byte{ZiplistEnd})
	return
}

func (zl *Ziplist) ExtractEncoded() (encoded []byte, ln int16,
	headprevlen, taillen int32) {

	head := zl.Zlhead()
	tail := zl.Zltail()
	end := zl.Zlbytes()

	encoded = []byte(*zl)[head:end]
	ln = zl.Zllen()
	headprevlen = zl.PrevLen(head)
	taillen = zl.entryLen(tail)
	return
}

func (zl *Ziplist) expand(offset, size int32) {
	s := make([]byte, size, size)
	(*zl) = append((*zl), s...)
	copy([]byte(*zl)[offset+size:], []byte(*zl)[offset:])
	return
}

func (zl *Ziplist) write(offset int32, bytes []byte) {
	dst := []byte(*zl)[offset:]
	copy(dst, bytes)
}

const (
	ZiplistHead = 0
	ZiplistTail = 1
)

func (zl *Ziplist) PopLeft() {
	zl.RemoveHead(1, 0)
	return
}

func (zl *Ziplist) Pop() {
	zl.RemoveTail(1, 0)
	return
}

func (zl *Ziplist) RemoveHead(num, skipnum int16) (int16, int16, bool) {
	var removednum int16
	var pass bool
	if skipnum == 0 {
		removednum, pass = zl.removeAll(num)
		if pass {
			return removednum, 0, pass
		}
	}
	if skipnum >= zl.Zllen() {
		return 0, zl.Zllen(), true
	}

	var start, offset int32
	offset = zl.offsetHeadSkipN(skipnum)
	start = offset
	for num > 0 {
		offset += zl.entryLen(offset)
		num--
		removednum++
		if zl.atEnd(offset) {
			pass = true
			break
		}
	}

	// the prevlen of the subsequent elements is updated only
	// when there are subsequent entry.
	if !pass {
		pprevlen := zl.PrevLen(start)
		prevlen := zl.PrevLen(offset)
		if prevlen < 254 && pprevlen >= 254 {
			offset -= 4
		} else if prevlen >= 254 && pprevlen < 254 {
			offset += 4
		}
		zl.write(offset, encodePrevLen(pprevlen))
	}

	zl.shrink(start, offset)
	zl.AddZlbytes(-(offset - start))
	zl.AddZltail(-(offset - start))
	zl.AddZllen(-removednum)
	return removednum, skipnum, pass
}

func (zl *Ziplist) offsetHeadSkipN(n int16) int32 {
	offset := zl.Zlhead()
	for ; n > 0; n-- {
		offset += zl.entryLen(offset)
	}
	return offset
}

func (zl *Ziplist) RemoveTail(num, skipnum int16) (int16, int16, bool) {
	var removednum int16
	var pass bool
	if skipnum == 0 {
		removednum, pass = zl.removeAll(num)
		if pass {
			return removednum, 0, pass
		}
	}
	if skipnum >= zl.Zllen() {
		return 0, zl.Zllen(), true
	}

	var start, offset int32
	offset = zl.offsetTailSkipN(skipnum)
	start = offset
	removednum = 1
	for num > 1 {
		prevlen := zl.PrevLen(offset)
		if prevlen == 0 {
			pass = true
			break
		}
		offset -= prevlen
		num--
		removednum++
	}

	pprevlen := zl.PrevLen(start)
	start += zl.entryLen(start)
	zl.shrink(offset, start)
	zl.AddZlbytes(-(start - offset))
	zl.AddZltail(-pprevlen)
	zl.AddZllen(-removednum)
	return removednum, skipnum, pass
}

func (zl *Ziplist) offsetTailSkipN(n int16) int32 {
	offset := zl.Zltail()
	for ; n > 0; n-- {
		offset -= zl.PrevLen(offset)
	}
	return offset
}

func (zl *Ziplist) removeAll(num int16) (int16, bool) {
	num = util.Cond(num > zl.Zllen(), zl.Zllen(), num)
	if num == zl.Zllen() {
		zl = NewZiplist()
		return num, true
	}
	return 0, false
}

func (zl *Ziplist) atEnd(offset int32) bool {
	return []byte(*zl)[offset] == 255
}

func (zl *Ziplist) shrink(start, end int32) {
	(*zl) = append((*zl)[:start], (*zl)[end:]...)
}

func (zl *Ziplist) Index(idx int32) ([]byte, bool) {
	var entry []byte
	iter := NewZiplistIterator(zl)
	for idx >= 0 && iter.HasNext() {
		entry = iter.Next()
		idx--
	}

	if idx >= 0 {
		return nil, false
	}
	return entry, true
}

type ZiplistIterator struct {
	zl     *Ziplist
	offset int32
	idx    int16
}

func NewZiplistIterator(zl *Ziplist) *ZiplistIterator {
	offset := int32(ZiplistHeaderSize)
	return &ZiplistIterator{
		zl:     zl,
		offset: offset,
	}
}

func (iter *ZiplistIterator) HasNext() bool {
	return !iter.zl.atEnd(iter.offset)
}

func (iter *ZiplistIterator) Next() []byte {
	entry, entrysize := iter.zl.DecodeEntry(iter.offset)
	iter.offset += entrysize
	iter.idx++
	return entry
}

func (iter *ZiplistIterator) Offset() int32 {
	return iter.offset
}

func (iter *ZiplistIterator) Index() int16 {
	return iter.idx
}
