package list

import (
	"encoding/binary"
	"math"
	"strconv"
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

func NewZiplist() ziplist {
	bytes := int32(ziplistHeaderSize + ziplistEndSize)

	zl := ziplist(make([]byte, bytes, bytes))
	zl.setZlbytes(bytes)
	zl.setZltail(ziplistHeaderSize)
	zl[bytes-1] = ziplistEnd
	return zl
}

func (zl *ziplist) setZlbytes(bytes int32) {
	binary.LittleEndian.PutUint32([]byte(*zl)[:4], uint32(bytes))
}

func (zl *ziplist) zlbytes() int32 {
	return int32(binary.LittleEndian.Uint32([]byte(*zl)[:4]))
}

func (zl *ziplist) setZltail(bytes int32) {
	binary.LittleEndian.PutUint32([]byte(*zl)[4:8], uint32(bytes))
}

func (zl *ziplist) zltail() int32 {
	return int32(binary.LittleEndian.Uint32([]byte(*zl)[4:8]))
}

func (zl *ziplist) setZllen(_len int16) {
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

func (zl *ziplist) decodeEntry(offset int32) (entry any, entrysize int32) {
	prevlensize := zl.prevLenSize(iter.offset)
	lensize, _len := zl.decodeEntryEncoding(iter.offset + prevlensize)
	_type, lensize, _len := decodeEntryEncoding(offset)
	conoffset := offset + prevlensize + lensize
	if _type == strType {
		entry = []byte(*zl)[conoffset : conoffset+_len]
	} else {
		if _len == 2 {
			entry = int32(binary.BigEndian.Uint16([]byte(*zl)[conoffset : conoffset+_len]))
		} else if _len == 4 {
			entry = int32(binary.BigEndian.Uint32([]byte(*zl)[conoffset : conoffset+_len]))
		} else if _len == 8 {
			entry = binary.BigEndian.Uint64([]byte(*zl)[conoffset : conoffset+_len])
		}
	}
	entrysize = conoffset + _len
	return
}

const (
	strType int8 = 0
	intType int8 = 1
)

func (zl *ziplist) decodeEntryEncoding(offset int32) (_type int8, lensize, _len int32) {
	_type := []byte(*zl)[offset] & zipStrMask
	encoding := []byte(*zl)[offset : offset+4]
	if _type < zipStrMask { // string
		_type = strType
		lensize, _len = zipStrSize(_type, encoding)
	} else { // int
		_type = intType
		lensize, _len = zipIntSize(_type, encoding)
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
	case zipInt24b:
		return 1, 3
	case zipInt32b:
		return 1, 4
	case zipInt64b:
		return 1, 8
	default:
	}
	return 0, 0
}

func (zl *ziplist) encodeEntry(prevlen int32, content []byte) []byte {
	entry := make([]byte, 0)
	entry = append(entry, encodePrevLen(prevlen))
	_type, _len, encoding := zl.encodeEntryEncoding(content)
	entry = append(entry, encoding...)
	if _type == strType {
		entry = append(entry, content...)
	} else {
		num, _ := strconv.ParseInt(string(content), 10, 32)
		if _len == 8 {
			entry = append(entry, content...)
		} else if _len == 16 {
			buf := make([]byte, 2)
			binary.BigEndian.PutUint16(buf, uint16(num))
			entry = append(entry, buf...)
		} else if _len == 32 {
			buf := make([]byte, 4)
			binary.BigEndian.PutUint32(buf, uint32(num))
			entry = append(entry, buf...)
		}
	}
	return entry
}

func (zl *ziplist) encodeEntryEncoding(content []byte) (int8, int32, []byte) {
	if len(content) < 32 {
		if num, err := strconv.ParseInt(string(content), 10, 32); err == nil {
			// <encoding-num-len>
			return intType, zipIntEncoding(int32(num))
		}
	}
	return strType, zipStrEncoding(content)
}

func zipIntEncoding(num int32) (int32, []byte) {
	var buf []byte
	switch {
	case num >= 0 && num <= 12:
		return int32(0), []byte{byte(0xf1 + num)}
	case num >= math.MinInt8 && num <= math.MaxInt8:
		return int32(8), []byte{zipInt8b}
	case num >= math.MinInt16 && num <= math.MaxInt16:
		buf = make([]byte, 2)
		binary.BigEndian.PutUint16(buf, uint16(num))
		return int32(16), buf
	case num >= math.MinInt32 && num <= math.MaxInt32:
		buf = make([]byte, 4)
		binary.BigEndian.PutUint32(buf, uint32(num))
		return int32(32), buf
	default:
	}
	return int32(0), buf
}

func zipStrEncoding(content []byte) (int32, []byte) {
	_len := len(content)
	switch {
	case _len <= 0x3f:
		// 0x3f 0011 1111
		// |00pppppp| - 1 byte
		return _, []byte{byte(zipStr06b + _len)}
	case _len <= 0x3fff:
		// 0x3fff 0011 1111 1111 1111
		// |01pppppp|qqqqqqqq| - 2 bytes
		return _, []byte{byte(zipStr14b | (_len>>8)&0x3f), byte(_len & 0xff)}
	default:
	}
	// |10000000|qqqqqqqq|rrrrrrrr|ssssssss|tttttttt| - 5 bytes
	return _, []byte{byte(zipStr32b), byte((_len >> 24) & 0xff),
		byte((_len >> 16) & 0xff), byte((_len >> 8) & 0xff), byte(_len & 0xff)}
}

func (zl *ziplist) tailEntry() []byte {
	tailOffset := zl.zltail()
	return []byte(*zl)[tailOffset:]
}

func (zl *ziplist) entryLen(offset int32) int32 {
	prevlensize := prevLenSize(offset)
	lensize, _len := zl.decodeEntryEncoding(offset + prevlensize)
	return prevlensize + lensize + _len
}

func (zl *ziplist) prevLenSize(offset int32) int32 {
	return prevLenSize(zl.prevLen(offset))
}

func prevLenSize(prevlen int32) int32 {
	var prevlensize int32 = 1
	if zl.prevLen(offset) > 254 {
		offset += 4
	}
	return prevlensize
}

func encodePrevLen(size int32) []byte {
	if size < 254 {
		return []byte{byte(size)}
	}
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(size))
	return buf
}

func (zl *ziplist) prevLen(offset int32) int32 {
	if []byte(*zl)[offset] != 0xFE {
		return int32([]byte(*zl)[offset])
	}
	return int32(binary.LittleEndian.Uint32([]byte(*zl)[offset+1 : offset+5]))
}

func (zl *ziplist) Push(content []byte) {
	offset := zl.zltail()
	zl.insert(offset, content)
	return
}

func (zl *ziplist) PushLeft(content []byte) {
	offset := ziplistHeaderSize
	zl.insert(int32(offset), content)
	return
}

func (zl *ziplist) insert(offset int32, content []byte) {
	var prevlen, conlen, entrysize, nextdiff int32

	b := ([]byte)(*zl)[offset]
	if b == ziplistEnd && zl.zllen() > 0 { // end
		prevlen = zl.entryLen(zl.zltail())
	}

	conlen = int32(len(content))

	// calculate the number of bytes required to insert the content
	// 1. prevlen
	// 2. encoding
	// 3. len(content)

	// does the insert cause changes in the prevlen of next entry
	// +4 or not
	if b != ziplistEnd && zl.zllen() > 0 { // header
		if conlen > 254 {
			nextdiff = 4
		}
	}

	entrysize = 1 + conlen
	if entrysize > 254 {
		entrysize += 4
	}

	// expand ziplist to store content
	zl.expand(offset, entrysize+nextdiff)

	// store <prevlen><encoding><data> for content
	entry := zl.encodeEntry(content)
	zl.write(offset, entry)

	// reset prevlen for next entry
	if nextdiff > 0 {
		zl.write(offset+entrysize, []byte{0xFE})
		zl.write(offset+entrysize+1, encodePrevLen(entrysize))
	} else if b != ziplistEnd && zl.zllen() == 0 {
		zl.write(offset+entrysize, []byte{byte(entrysize)})
	}
	return
}

func (zl *ziplist) expand(offset, size int32) {
	s := make([]byte, size, size)
	(*zl) = append((*zl), s...)
	copy([]byte(*zl)[offset+size:], []byte(*zl)[offset:])
	return
}

func (zl *ziplist) write(offset int32, b []byte) {
	dst := []byte(*zl)[offset:]
	copy(dst, b)
}

func entryLenSize(_len int32) int32 {
	if _len > 254 {
		return 5
	}
	return 1
}

func (zl *ziplist) Pop() {
	offset := zl.zltail()
	zl.remove(offset, 1)
	return
}

func (zl *ziplist) PopLeft() {
	offset := ziplistHeaderSize
	zl.remove(int32(offset), int32(1))
	return
}

func (zl *ziplist) remove(offset, num int32) {
	var removednum int16
	var i int32
	for ; i < num && zl.isEnd(offset); i++ {
		offset += zl.entryLen(offset)
		removednum++
	}

	// 更新后一个元素的prevlen
	start := offset
	prevoffset := start - zl.prevLen(start)
	prevlen := zl.entryLen(prevoffset)
	if !zl.isEnd(offset) {
		nprevlen := zl.prevLen(offset)
		if prevlen < 255 && nprevlen >= 255 {
			offset += 4
		} else if prevlen >= 255 && nprevlen < 255 {
			offset -= 4
		}
		zl.write(offset, encodePrevLen(prevlen))
	} else {
		zl.setZltail(prevoffset)
	}

	zl.setZlbytes(zl.zlbytes() - (offset - start))
	zl.setZllen(zl.zllen() - removednum)

	zl.shrink(start, offset)
	return
}

func (zl *ziplist) isEnd(offset int32) bool {
	return []byte(*zl)[offset] == 255
}

func (zl *ziplist) shrink(start, end int32) {
	(*zl) = append((*zl)[:start], (*zl)[end:]...)
}

type ziplistIterator struct {
	zl     ziplist
	offset int32
}

func NewZiplistIterator(zl ziplist) ziplistIterator {
	offset := int32(ziplistHeaderSize)
	return ziplistIterator{
		zl:     zl,
		zllen:  zllen,
		offset: offset,
	}
}

func (iter *ziplistIterator) hasNext() bool {
	return !iter.zl.isEnd()
}

func (iter *ziplistIterator) next() any {
	entry, entrysize := iter.zl.decodeEntry(offset)
	iter.offset += entrysize
	return entry
}

func (zl *ziplist) Index(idx int32) (any, bool) {
	var entry any
	iter := NewZiplistIterator(zl)
	for idx >= 0 && iter.hasNext() {
		entry = iter.next()
		idx--
	}

	if idx >= 0 {
		return nil, false
	}
	return entry, true
}
