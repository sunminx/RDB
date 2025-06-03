package datastruct

import (
	"encoding/binary"
	"math"
	"strconv"

	. "github.com/sunminx/RDB/pkg/util"
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
	ZiplistHeaderSize = uint32(4*2 + 2)
	ZiplistEndSize    = uint32(1)
	ZiplistEnd        = byte(255)
)

func NewZiplist() *Ziplist {
	metaLen := ZiplistHeaderSize + ZiplistEndSize
	zl := Ziplist(make([]byte, metaLen, metaLen))
	zl.SetLen(0)
	zl.SetBytes(metaLen)
	zl.SetTailOffset(ZiplistHeaderSize)
	zl[metaLen-1] = ZiplistEnd
	return &zl
}

func (zl *Ziplist) DeepCopy() *Ziplist {
	b := make([]byte, 0, zl.Bytes())
	copy(b, []byte(*zl))
	nzl := Ziplist(b)
	return &nzl
}

func (zl *Ziplist) SetBytes(bytes uint32) {
	binary.LittleEndian.PutUint32([]byte(*zl)[:4], bytes)
}

func (zl *Ziplist) addBytes(n uint32) {
	n += zl.Bytes()
	binary.LittleEndian.PutUint32([]byte(*zl)[:4], n)
}

func (zl *Ziplist) Bytes() uint32 {
	return binary.LittleEndian.Uint32([]byte(*zl)[:4])
}

func (zl *Ziplist) SetTailOffset(tail uint32) {
	binary.LittleEndian.PutUint32([]byte(*zl)[4:8], tail)
}

func (zl *Ziplist) addTailOffset(n uint32) {
	n += zl.TailOffset()
	binary.LittleEndian.PutUint32([]byte(*zl)[4:8], n)
}

func (zl *Ziplist) TailOffset() uint32 {
	return binary.LittleEndian.Uint32([]byte(*zl)[4:8])
}

func (zl *Ziplist) HeadOffset() uint32 {
	return ZiplistHeaderSize
}

func (zl *Ziplist) SetLen(ln uint16) {
	binary.LittleEndian.PutUint16([]byte(*zl)[8:10], ln)
}

func (zl *Ziplist) addLen(n uint16) {
	n += zl.Len()
	binary.LittleEndian.PutUint16([]byte(*zl)[8:10], n)
}

func (zl *Ziplist) Len() uint16 {
	return binary.LittleEndian.Uint16([]byte(*zl)[8:10])
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
func (zl *Ziplist) DecodeEntry(offset uint32) (entry []byte, size uint32) {
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
	size = prevlensize + lensize + ln
	return
}

const (
	strType int8 = 0
	intType int8 = 1
)

func (zl *Ziplist) decodeEntryEncoding(start uint32) (int8, uint32, uint32) {
	// typeByte: first two bit of byte
	_type := []byte(*zl)[start] & zipStrMask
	bytes := zl.Bytes()
	end := Cond(start+5 > bytes, bytes, start+5)
	encoding := []byte(*zl)[start:end]
	if _type < zipStrMask {
		lenSize, _len := zipStrSize(_type, encoding)
		return strType, lenSize, _len
	}
	lenSize, _len := zipIntSize([]byte(*zl)[start], encoding)
	return intType, lenSize, _len
}

func zipStrSize(_type byte, encoding []byte) (uint32, uint32) {
	switch _type {
	case zipStr06b:
		// 0x3f 00111111
		// |00pppppp| - 1 byte
		// The last 6 bits of "encoding" encode the length of the string.
		return 1, uint32(encoding[0]) & 0x3f
	case zipStr14b:
		// |01pppppp|qqqqqqqq| - 2 bytes
		// The last 14 bits of "encoding" encode the length of the string.
		return 2, (uint32(encoding[0])&0x3f)<<8 | uint32(encoding[1])
	case zipStr32b:
		// |10000000|qqqqqqqq|rrrrrrrr|ssssssss|tttttttt| - 5 bytes
		// The last 32 bits of "encoding" encode the length of the string.
		// The 6 lower bits of the first byte are not used and are set to zero.
		return 5, uint32(encoding[1])<<24 | uint32(encoding[2])<<16 | uint32(encoding[3])<<8 | uint32(encoding[4])
	default:
		return 0, 0
	}
}

func zipIntSize(_type byte, _ []byte) (uint32, uint32) {
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
		return 0, 0
	}
}

func (zl *Ziplist) EncodeEntry(prevlen uint32, content []byte) []byte {
	entry := encodePrevLen(prevlen)
	_type, _, encoded := zl.encodeEntryEncoding(content)
	entry = append(entry, encoded...)
	if _type == strType {
		entry = append(entry, content...)
	}
	return entry
}

func (zl *Ziplist) encodeEntryEncoding(entry []byte) (int8, uint32, []byte) {
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
func ZiplistEntryEncodeLen(ln uint32) uint32 {
	var overhead uint32

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

func zipIntEncoding(num int32) (uint32, []byte) {
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
		return 0, buf
	}
}

func zipStrEncoding(entry []byte) (uint32, []byte) {
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
	n := zl.TailOffset()
	return []byte(*zl)[n:]
}

func (zl *Ziplist) entryLen(offset uint32) uint32 {
	prevlensize := prevLenSize(offset)
	_, lensize, ln := zl.decodeEntryEncoding(offset + prevlensize)
	return prevlensize + lensize + ln
}

func (zl *Ziplist) prevLenSize(offset uint32) uint32 {
	return prevLenSize(zl.PrevLen(offset))
}

func prevLenSize(prevlen uint32) uint32 {
	prevlensize := uint32(1)
	if prevlen >= 254 {
		prevlensize += 4
	}
	return prevlensize
}

func encodePrevLen(size uint32) []byte {
	if size < 254 {
		return []byte{byte(size)}
	}
	buf := make([]byte, 5, 5)
	buf[0] = 0xFE
	binary.LittleEndian.PutUint32(buf[1:], size)
	return buf
}

func (zl *Ziplist) PrevLen(offset uint32) uint32 {
	if []byte(*zl)[offset] != 0xFE {
		return uint32([]byte(*zl)[offset])
	}
	return binary.LittleEndian.Uint32([]byte(*zl)[offset+1 : offset+5])
}

func (zl *Ziplist) ReplaceAtIndex(index uint16, entry []byte) {
	index = Cond(index < 0, 0, index)
	n := zl.Len()
	index = Cond(index >= n, n-1, index)
	offset := zl.offsetHeadSkipN(index)
	prevLen := zl.PrevLen(offset)
	encoded := zl.EncodeEntry(prevLen, entry)
	encodedLen := uint32(len(encoded))
	oldEncodedLen := zl.entryLen(offset)
	if encodedLen > oldEncodedLen {
		zl.expand(offset, encodedLen-oldEncodedLen)
	} else if encodedLen < oldEncodedLen {
		zl.shrink(offset+(encodedLen-oldEncodedLen), offset)
	}
	zl.write(offset, encoded)
}

func (zl *Ziplist) Push(entry []byte) {
	endOffset := zl.Bytes() - 1
	zl.insert(endOffset, entry)
}

func (zl *Ziplist) PushLeft(entry []byte) {
	offset := ZiplistHeaderSize
	zl.insert(offset, entry)
}

func (zl *Ziplist) insert(offset uint32, content []byte) {
	var prevLen, nextDiff uint32
	bytes := zl.Bytes()
	_len := zl.Len()

	firstByte := ([]byte)(*zl)[offset]
	if firstByte == ZiplistEnd && _len > 0 { // end
		prevLen = zl.entryLen(zl.TailOffset())
		zl.SetTailOffset(offset)
	}
	// calculate the number of bytes required to insert the entry
	// 1. prevLen
	// 2. encoding
	// 3. len(entry)

	// does the insert cause changes in the prevLen of next entry
	// +4 or not

	entry := zl.EncodeEntry(prevLen, content)
	entrySize := uint32(len(entry))
	if firstByte != ZiplistEnd && _len > 0 { // header
		if entrySize >= 254 {
			nextDiff = 4
		}
		zl.SetTailOffset(offset + entrySize)
	}
	zl.expand(offset, entrySize+nextDiff)
	// store <prevLen><encoding><data> for entry
	zl.write(offset, entry)
	// reset prevLen for next entry
	if nextDiff > 0 {
		zl.write(offset+entrySize, encodePrevLen(entrySize))
	} else if firstByte != ZiplistEnd && _len == 0 {
		zl.write(offset+entrySize, []byte{byte(entrySize)})
	}
	bytes += entrySize + nextDiff
	zl.write(bytes-1, []byte{ZiplistEnd})
	zl.SetLen(_len + 1)
	zl.SetBytes(bytes)
	return
}

func (zl *Ziplist) InsertEncoded(offset uint32, encoded []byte, _len uint16, headPrevLen, tailLen uint32) {
	// 头部插入 更新原来第一个entry的prevlen
	// 尾部插入 更新插入的encoded的第一个entry的prevlen
	encodedLen := uint32(len(encoded))
	prevLen := zl.PrevLen(offset)
	if !zl.atEnd(offset) {
		if prevLen < 254 && tailLen >= 254 {
			encodedLen += 4
		} else if prevLen >= 254 && tailLen < 254 {
			encodedLen -= 4
		}
		zl.expand(offset, encodedLen)
		zl.write(offset, encoded)
		zl.write(uint32(offset+encodedLen), encodePrevLen(tailLen))
	} else {
		if prevLen < 254 && headPrevLen >= 254 {
			encodedLen += 4
		} else if prevLen >= 254 && headPrevLen < 254 {
			encodedLen -= 4
		}
		zl.expand(offset, encodedLen)
		zl.write(offset, encoded)
		zl.write(offset, encodePrevLen(prevLen))
	}

	zl.addBytes(encodedLen)
	zl.addLen(_len)
	zl.addTailOffset(encodedLen)
	zl.write(zl.Bytes()-1, []byte{ZiplistEnd})
}

func (zl *Ziplist) ExtractEncoded() ([]byte, uint16, uint32, uint32) {
	head := zl.HeadOffset()
	tail := zl.TailOffset()
	end := zl.Bytes()

	encoded := []byte(*zl)[head:end]
	_len := zl.Len()
	headPrevLen := zl.PrevLen(head)
	tailLen := zl.entryLen(tail)
	return encoded, _len, headPrevLen, tailLen
}

func (zl *Ziplist) expand(offset, size uint32) {
	s := make([]byte, size, size)
	(*zl) = append((*zl), s...)
	copy([]byte(*zl)[offset+size:], []byte(*zl)[offset:])
}

func (zl *Ziplist) write(offset uint32, bytes []byte) {
	dst := []byte(*zl)[offset:]
	copy(dst, bytes)
}

const (
	ZiplistHead = 0
	ZiplistTail = 1
)

func (zl *Ziplist) PopLeft() []byte {
	removes, _, _ := zl.RemoveHead(1, 0)
	if removes != nil && len(removes) > 0 {
		return removes[0]
	}
	return nil
}

func (zl *Ziplist) Pop() []byte {
	removes, _, _ := zl.RemoveTail(1, 0)
	if removes != nil && len(removes) > 0 {
		return removes[0]
	}
	return nil
}

func (zl *Ziplist) RemoveHead(num, skipnum uint16) ([][]byte, uint16, bool) {
	// If there are no entry that need to be skipped, trying to delete zl directly.
	if skipnum == 0 {
		removes, pass := zl.removeAll(num)
		if pass {
			return removes, 0, pass
		}
	}
	// If the skipnum exceeds the zllen, it indicates that no entry in zl can be deleted.
	n := zl.Len()
	if skipnum >= n {
		return nil, n, true
	}

	var (
		removedNum = uint16(0)
		pass       = false
		removes    = make([][]byte, 0)
		offset     = zl.offsetHeadSkipN(skipnum)
		start      = offset
	)
	for num > 0 {
		entry, entrySize := zl.DecodeEntry(offset)
		nentry := make([]byte, len(entry))
		copy(nentry, entry)
		removes = append(removes, nentry)
		offset += entrySize
		num--
		removedNum++
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
	zl.addBytes(-(offset - start))
	zl.addTailOffset(-(offset - start))
	zl.addLen(-removedNum)
	return removes, skipnum, pass
}

func (zl *Ziplist) offsetHeadSkipN(n uint16) uint32 {
	offset := zl.HeadOffset()
	for ; n > 0; n-- {
		offset += zl.entryLen(offset)
	}
	return offset
}

func (zl *Ziplist) RemoveTail(num, skipnum uint16) ([][]byte, uint16, bool) {
	// If there are no entry that need to be skipped, trying to delete zl directly.
	if skipnum == 0 {
		removes, pass := zl.removeAll(num)
		if pass {
			return removes, 0, pass
		}
	}
	// If the skipnum exceeds the zllen, it indicates that no entry in zl can be deleted.

	n := zl.Len()
	if skipnum >= n {
		return nil, n, true
	}

	var (
		removedNum = uint16(0)
		pass       = false
		removes    = make([][]byte, 0)
		offset     = zl.offsetTailSkipN(skipnum)
		start      = offset
	)
	for num > 0 {
		entry, _ := zl.DecodeEntry(offset)
		nentry := make([]byte, len(entry))
		copy(nentry, entry)
		removes = append(removes, nentry)
		num--
		removedNum++
		prevlen := zl.PrevLen(offset)
		if prevlen == 0 {
			pass = true
			break
		}
		offset -= prevlen
	}

	pprevlen := zl.PrevLen(start)
	start += zl.entryLen(start)
	zl.shrink(offset, start)
	zl.addBytes(-(start - offset))
	zl.addTailOffset(-pprevlen)
	zl.addLen(-removedNum)
	return removes, skipnum, pass
}

func (zl *Ziplist) offsetTailSkipN(n uint16) uint32 {
	offset := zl.TailOffset()
	for ; n > 0; n-- {
		offset -= zl.PrevLen(offset)
	}
	return offset
}

func (zl *Ziplist) removeAll(num uint16) ([][]byte, bool) {
	n := zl.Len()
	num = Cond(num > n, n, num)
	if num == n {
		entries := zl.getAllEntries()
		return entries, true
	}
	return nil, false
}

func (zl *Ziplist) getAllEntries() [][]byte {
	entries := make([][]byte, 0, zl.Len())
	iter := NewZiplistIterator(zl)
	for iter.HasNext() {
		entry := iter.Next()
		nentry := make([]byte, len(entry))
		copy(nentry, entry)
		entries = append(entries, nentry)
	}
	return entries
}

func (zl *Ziplist) atEnd(offset uint32) bool {
	return []byte(*zl)[offset] == 255
}

func (zl *Ziplist) shrink(start, end uint32) {
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
	offset uint32
	idx    uint16
}

func NewZiplistIterator(zl *Ziplist) *ZiplistIterator {
	offset := uint32(ZiplistHeaderSize)
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

func (iter *ZiplistIterator) Offset() uint32 {
	return iter.offset
}

func (iter *ZiplistIterator) Index() uint16 {
	return iter.idx
}
