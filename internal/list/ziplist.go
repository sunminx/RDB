package list

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
type ziplist []byte

const (
	ziplistHeaderSize = 4*2 + 2
	ziplistEndSize    = 1

	ziplistEnd = 255
)

func NewZiplist() *ziplist {
	bytes := int32(ziplistHeaderSize + ziplistEndSize)

	zl := ziplist(make([]byte, bytes, bytes))
	zl.setZlbytes(bytes)
	zl.setZltail(ziplistHeaderSize)
	zl[bytes-1] = ziplistEnd
	return &zl
}

func (zl *ziplist) setZlbytes(bytes int32) {
	binary.LittleEndian.PutUint32([]byte(*zl)[:4], uint32(bytes))
}

func (zl *ziplist) addZlbytes(bytes int32) {
	bytes += zl.zlbytes()
	binary.LittleEndian.PutUint32([]byte(*zl)[:4], uint32(bytes))
}

func (zl *ziplist) zlbytes() int32 {
	return int32(binary.LittleEndian.Uint32([]byte(*zl)[:4]))
}

func (zl *ziplist) setZltail(tail int32) {
	binary.LittleEndian.PutUint32([]byte(*zl)[4:8], uint32(tail))
}

func (zl *ziplist) addZltail(tail int32) {
	tail += zl.zltail()
	binary.LittleEndian.PutUint32([]byte(*zl)[4:8], uint32(tail))
}

func (zl *ziplist) zlhead() int32 {
	return ziplistHeaderSize
}

func (zl *ziplist) zltail() int32 {
	return int32(binary.LittleEndian.Uint32([]byte(*zl)[4:8]))
}

func (zl *ziplist) setZllen(_len int16) {
	binary.LittleEndian.PutUint16([]byte(*zl)[8:10], uint16(_len))
}

func (zl *ziplist) addZllen(_len int16) {
	_len += zl.zllen()
	binary.LittleEndian.PutUint16([]byte(*zl)[8:10], uint16(_len))
}

func (zl *ziplist) zllen() int16 {
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
func (zl *ziplist) decodeEntry(offset int32) (entry []byte, entrysize int32) {
	prevlensize := zl.prevLenSize(offset)
	_type, lensize, _len := zl.decodeEntryEncoding(offset + prevlensize)
	conoffset := offset + prevlensize + lensize
	if _type == strType {
		entry = []byte(*zl)[conoffset : conoffset+_len]
	} else {
		if _len == 0 {
			encoding := []byte(*zl)[offset+prevlensize : offset+prevlensize+1]
			num := (encoding[0] & 0x0F) - 1
			entry = []byte(strconv.FormatInt(int64(num), 10))
		} else if _len == 1 {
			num := []byte(*zl)[conoffset : conoffset+_len]
			entry = []byte(strconv.FormatInt(int64(num[0]), 10))
		} else if _len == 2 {
			num := binary.LittleEndian.Uint16([]byte(*zl)[conoffset : conoffset+_len])
			entry = []byte(strconv.FormatInt(int64(num), 10))
		} else if _len == 4 {
			num := binary.LittleEndian.Uint32([]byte(*zl)[conoffset : conoffset+_len])
			entry = []byte(strconv.FormatInt(int64(num), 10))
		} else if _len == 8 {
			num := binary.LittleEndian.Uint64([]byte(*zl)[conoffset : conoffset+_len])
			entry = []byte(strconv.FormatInt(int64(num), 10))
		}
	}
	entrysize = prevlensize + lensize + _len
	return
}

const (
	strType int8 = 0
	intType int8 = 1
)

func (zl *ziplist) decodeEntryEncoding(offset int32) (strorint int8,
	lensize, _len int32) {
	// _type: first two bit of byte
	_type := []byte(*zl)[offset] & zipStrMask
	encoding := []byte(*zl)[offset : offset+4]
	if _type < zipStrMask { // string
		strorint = strType
		lensize, _len = zipStrSize(_type, encoding)
	} else { // int
		strorint = intType
		lensize, _len = zipIntSize([]byte(*zl)[offset], encoding)
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

func (zl *ziplist) encodeEntry(prevlen int32, content []byte) []byte {
	entry := make([]byte, 0)
	entry = append(entry, encodePrevLen(prevlen)...)
	_type, _, encoded := zl.encodeEntryEncoding(content)
	entry = append(entry, encoded...)
	if _type == strType {
		entry = append(entry, content...)
	}
	return entry
}

func (zl *ziplist) encodeEntryEncoding(entry []byte) (int8, int32, []byte) {
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

// entryEncodeLen estimate the number of bytes occupied by the entry.
func entryEncodeLen(_len int32) int32 {
	var overhead int32

	if _len < 254 {
		overhead = 1
	} else {
		overhead = 5
	}

	if _len < 64 {
		overhead += 1
	} else if _len < 16384 {
		overhead += 2
	} else {
		overhead += 5
	}

	return overhead + _len
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
	_len := len(entry)
	switch {
	case _len <= 0x3f:
		// 0x3f 0011 1111
		// |00pppppp| - 1 byte
		return 1, []byte{byte(zipStr06b + _len)}
	case _len <= 0x3fff:
		// 0x3fff 0011 1111 1111 1111
		// |01pppppp|qqqqqqqq| - 2 bytes
		return 2, []byte{byte(zipStr14b | (_len>>8)&0x3f), byte(_len & 0xff)}
	default:
	}
	// |10000000|qqqqqqqq|rrrrrrrr|ssssssss|tttttttt| - 5 bytes
	return 5, []byte{zipStr32b, byte((_len >> 24) & 0xff),
		byte((_len >> 16) & 0xff), byte((_len >> 8) & 0xff), byte(_len & 0xff)}
}

func (zl *ziplist) tailEntry() []byte {
	tailOffset := zl.zltail()
	return []byte(*zl)[tailOffset:]
}

func (zl *ziplist) entryLen(offset int32) int32 {
	prevlensize := prevLenSize(offset)
	_, lensize, _len := zl.decodeEntryEncoding(offset + prevlensize)
	return prevlensize + lensize + _len
}

func (zl *ziplist) prevLenSize(offset int32) int32 {
	return prevLenSize(zl.prevLen(offset))
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

func (zl *ziplist) prevLen(offset int32) int32 {
	if []byte(*zl)[offset] != 0xFE {
		return int32([]byte(*zl)[offset])
	}
	return int32(binary.LittleEndian.Uint32([]byte(*zl)[offset+1 : offset+5]))
}

func (zl *ziplist) Push(entry []byte) {
	offset := zl.zlbytes() - 1
	zl.insert(offset, entry)
	return
}

func (zl *ziplist) PushLeft(entry []byte) {
	offset := ziplistHeaderSize
	zl.insert(int32(offset), entry)
	return
}

func (zl *ziplist) insert(offset int32, content []byte) {
	var prevlen, nextdiff int32
	zlbytes := zl.zlbytes()
	zllen := zl.zllen()
	zltail := zl.zltail()

	firstByte := ([]byte)(*zl)[offset]
	if firstByte == ziplistEnd && zl.zllen() > 0 { // end
		prevlen = zl.entryLen(zltail)
		zl.setZltail(offset)
	}
	// calculate the number of bytes required to insert the entry
	// 1. prevlen
	// 2. encoding
	// 3. len(entry)

	// does the insert cause changes in the prevlen of next entry
	// +4 or not

	entry := zl.encodeEntry(prevlen, content)
	entrysize := int32(len(entry))
	if firstByte != ziplistEnd && zl.zllen() > 0 { // header
		if entrysize >= 254 {
			nextdiff = 4
		}
		zl.setZltail(offset + entrysize)
	}
	zl.expand(offset, entrysize+nextdiff)
	// store <prevlen><encoding><data> for entry
	zl.write(offset, entry)
	// reset prevlen for next entry
	if nextdiff > 0 {
		zl.write(offset+entrysize, encodePrevLen(entrysize))
	} else if firstByte != ziplistEnd && zl.zllen() == 0 {
		zl.write(offset+entrysize, []byte{byte(entrysize)})
	}
	zlbytes += entrysize + nextdiff
	zl.write(zlbytes-1, []byte{ziplistEnd})
	zl.setZllen(zllen + 1)
	zl.setZlbytes(zlbytes)
	return
}

func (zl *ziplist) insertEncoded(offset int32, encoded []byte,
	_len int16, headprevlen, taillen int32) {

	encodedlen := int32(len(encoded))
	prevlen := zl.prevLen(offset)

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

	zl.addZlbytes(encodedlen + nextdiff)
	zl.addZllen(_len)
	zl.addZltail(encodedlen + nextdiff)
	zl.write(zl.zlbytes()-1, []byte{ziplistEnd})
	return
}

func (zl *ziplist) extractEncoded() (encoded []byte, _len int16,
	headprevlen, taillen int32) {

	head := zl.zlhead()
	tail := zl.zltail()
	end := zl.zlbytes()

	encoded = []byte(*zl)[head:end]
	_len = zl.zllen()
	headprevlen = zl.prevLen(head)
	taillen = zl.entryLen(tail)
	return
}

func (zl *ziplist) expand(offset, size int32) {
	s := make([]byte, size, size)
	(*zl) = append((*zl), s...)
	copy([]byte(*zl)[offset+size:], []byte(*zl)[offset:])
	return
}

func (zl *ziplist) write(offset int32, bytes []byte) {
	dst := []byte(*zl)[offset:]
	copy(dst, bytes)
}

const (
	ziplistHead = 0
	ziplistTail = 1
)

func (zl *ziplist) PopLeft() {
	zl.removeHead(1, 0)
	return
}

func (zl *ziplist) Pop() {
	zl.removeTail(1, 0)
	return
}

func (zl *ziplist) removeHead(num, skipnum int16) (int16, int16, bool) {
	var removednum int16
	var pass bool
	if skipnum == 0 {
		removednum, pass = zl.removeAll(num)
		if pass {
			return removednum, 0, pass
		}
	}
	if skipnum >= zl.zllen() {
		return 0, zl.zllen(), true
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
		pprevlen := zl.prevLen(start)
		prevlen := zl.prevLen(offset)
		if prevlen < 254 && pprevlen >= 254 {
			offset -= 4
		} else if prevlen >= 254 && pprevlen < 254 {
			offset += 4
		}
		zl.write(offset, encodePrevLen(pprevlen))
	}

	zl.shrink(start, offset)
	zl.addZlbytes(-(offset - start))
	zl.addZltail(-(offset - start))
	zl.addZllen(-removednum)
	return removednum, skipnum, pass
}

func (zl *ziplist) offsetHeadSkipN(n int16) int32 {
	offset := zl.zlhead()
	for ; n > 0; n-- {
		offset += zl.entryLen(offset)
	}
	return offset
}

func (zl *ziplist) removeTail(num, skipnum int16) (int16, int16, bool) {
	var removednum int16
	var pass bool
	if skipnum == 0 {
		removednum, pass = zl.removeAll(num)
		if pass {
			return removednum, 0, pass
		}
	}
	if skipnum >= zl.zllen() {
		return 0, zl.zllen(), true
	}

	var start, offset int32
	offset = zl.offsetTailSkipN(skipnum)
	start = offset
	removednum = 1
	for num > 1 {
		prevlen := zl.prevLen(offset)
		if prevlen == 0 {
			pass = true
			break
		}
		offset -= prevlen
		num--
		removednum++
	}

	pprevlen := zl.prevLen(start)
	start += zl.entryLen(start)
	zl.shrink(offset, start)
	zl.addZlbytes(-(start - offset))
	zl.addZltail(-pprevlen)
	zl.addZllen(-removednum)
	return removednum, skipnum, pass
}

func (zl *ziplist) offsetTailSkipN(n int16) int32 {
	offset := zl.zltail()
	for ; n > 0; n-- {
		offset -= zl.prevLen(offset)
	}
	return offset
}

func (zl *ziplist) removeAll(num int16) (int16, bool) {
	num = util.CondInt16(num > zl.zllen(), zl.zllen(), num)
	if num == zl.zllen() {
		zl = NewZiplist()
		return num, true
	}
	return 0, false
}

func (zl *ziplist) atEnd(offset int32) bool {
	return []byte(*zl)[offset] == 255
}

func (zl *ziplist) shrink(start, end int32) {
	(*zl) = append((*zl)[:start], (*zl)[end:]...)
}

func (zl *ziplist) Index(idx int32) ([]byte, bool) {
	var entry []byte
	iter := newZiplistIterator(zl)
	for idx >= 0 && iter.hasNext() {
		entry = iter.next()
		idx--
	}

	if idx >= 0 {
		return nil, false
	}
	return entry, true
}

type ziplistIterator struct {
	zl     *ziplist
	offset int32
}

func newZiplistIterator(zl *ziplist) *ziplistIterator {
	offset := int32(ziplistHeaderSize)
	return &ziplistIterator{
		zl:     zl,
		offset: offset,
	}
}

func (iter *ziplistIterator) hasNext() bool {
	return !iter.zl.atEnd(iter.offset)
}

func (iter *ziplistIterator) next() []byte {
	entry, entrysize := iter.zl.decodeEntry(iter.offset)
	iter.offset += entrysize
	return entry
}
