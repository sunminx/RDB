package datastruct

import (
	"encoding/binary"
	"fmt"
	"math"
	"slices"
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

func (zl *Ziplist) Len() uint16                 { return binary.LittleEndian.Uint16([]byte(*zl)[8:10]) }
func (zl *Ziplist) Bytes() uint32               { return binary.LittleEndian.Uint32([]byte(*zl)[:4]) }
func (zl *Ziplist) HeadOffset() uint32          { return ZiplistHeaderSize }
func (zl *Ziplist) TailOffset() uint32          { return binary.LittleEndian.Uint32([]byte(*zl)[4:8]) }
func (zl *Ziplist) SetLen(ln uint16)            { binary.LittleEndian.PutUint16([]byte(*zl)[8:10], ln) }
func (zl *Ziplist) SetBytes(bytes uint32)       { binary.LittleEndian.PutUint32([]byte(*zl)[:4], bytes) }
func (zl *Ziplist) SetTailOffset(tail uint32)   { binary.LittleEndian.PutUint32([]byte(*zl)[4:8], tail) }
func (zl *Ziplist) addLen(ln uint16)            { zl.SetLen(ln + zl.Len()) }
func (zl *Ziplist) addBytes(bytes uint32)       { zl.SetBytes(bytes + zl.Bytes()) }
func (zl *Ziplist) addTailOffset(offset uint32) { zl.SetTailOffset(offset + zl.TailOffset()) }

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

// DecodeEntry decodes an entry located with offset.
func (zl *Ziplist) DecodeEntry(offset uint32) (entry []byte, size uint32) {
	prevLenSize := zl.prevLenSize(offset)
	typ, lenSize, ln := zl.decodeEntryEncoding(offset + prevLenSize)
	conOffset := offset + prevLenSize + lenSize
	if typ == strType {
		entry = []byte(*zl)[conOffset : conOffset+ln]
	} else {
		if ln == 0 {
			encoding := []byte(*zl)[offset+prevLenSize : offset+prevLenSize+1]
			num := (encoding[0] & 0x0F) - 1
			entry = []byte(strconv.FormatInt(int64(num), 10))
		} else if ln == 1 {
			num := []byte(*zl)[conOffset : conOffset+ln]
			entry = []byte(strconv.FormatInt(int64(num[0]), 10))
		} else if ln == 2 {
			num := binary.LittleEndian.Uint16([]byte(*zl)[conOffset : conOffset+ln])
			entry = []byte(strconv.FormatInt(int64(num), 10))
		} else if ln == 4 {
			num := binary.LittleEndian.Uint32([]byte(*zl)[conOffset : conOffset+ln])
			entry = []byte(strconv.FormatInt(int64(num), 10))
		} else if ln == 8 {
			num := binary.LittleEndian.Uint64([]byte(*zl)[conOffset : conOffset+ln])
			entry = []byte(strconv.FormatInt(int64(num), 10))
		}
	}
	size = prevLenSize + lenSize + ln
	return
}

const (
	strType int8 = 0
	intType int8 = 1
)

func (zl *Ziplist) decodeEntryEncoding(start uint32) (int8, uint32, uint32) {
	// typeByte: first two bit of byte
	typ := []byte(*zl)[start] & zipStrMask
	bytes := zl.Bytes()
	end := Cond(start+5 > bytes, bytes, start+5)
	encoding := []byte(*zl)[start:end]
	if typ < zipStrMask {
		lenSize, ln := zipStrSize(typ, encoding)
		return strType, lenSize, ln
	}
	lenSize, ln := zipIntSize([]byte(*zl)[start], encoding)
	return intType, lenSize, ln
}

func zipStrSize(typ byte, encoding []byte) (uint32, uint32) {
	switch typ {
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

func zipIntSize(typ byte, _ []byte) (uint32, uint32) {
	switch typ {
	case zipInt8b:
		return 1, 1
	case zipInt16b:
		return 1, 2
	case zipInt32b:
		return 1, 4
	case zipInt64b:
		return 1, 8
	default:
		if typ&0xf0 == 0xf0 {
			return 1, 0
		}
		return 0, 0
	}
}

// EncodeEntry encodes a content that will be insert into the ziplist.
func (zl *Ziplist) EncodeEntry(prevlen uint32, content []byte) []byte {
	entry := encodePrevLen(prevlen)
	typ, _, encoded := zl.encodeEntryEncoding(content)
	entry = append(entry, encoded...)
	if typ == strType {
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

// ZiplistEntryEncodeLen estimates the number of bytes occupied by the entry.
func ZiplistEntryEncodeLen(ln uint32) uint32 {
	overhead := uint32(0)
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

func (zl *Ziplist) entrySize(offset uint32) uint32 {
	prevLenSize := zl.prevLenSize(offset)
	_, lenSize, ln := zl.decodeEntryEncoding(offset + prevLenSize)
	return prevLenSize + lenSize + ln
}

func (zl *Ziplist) prevLenSize(offset uint32) uint32 {
	prevLenSize := uint32(1)
	prevLen := zl.PrevLen(offset)
	if prevLen >= 254 {
		prevLenSize += 4
	}
	return prevLenSize
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

// PrevLen returns length of the previous entry.
func (zl *Ziplist) PrevLen(offset uint32) uint32 {
	if []byte(*zl)[offset] != 0xFE {
		return uint32([]byte(*zl)[offset])
	}
	return binary.LittleEndian.Uint32([]byte(*zl)[offset+1 : offset+5])
}

// ReplaceAtIndex replaces the entry's content locate with index.
func (zl *Ziplist) ReplaceAtIndex(index uint16, entry []byte) {
	index = Cond(index < 0, 0, index)
	n := zl.Len()
	index = Cond(index >= n, n-1, index)
	offset := zl.offsetHeadSkipN(index)
	zl.ReplaceAtOffset(offset, entry)
}

// ReplaceAtIndex replaces the entry's content locate with offset.
func (zl *Ziplist) ReplaceAtOffset(offset uint32, entry []byte) {
	prevLen := zl.PrevLen(offset)
	encoded := zl.EncodeEntry(prevLen, entry)
	encodedLen := uint32(len(encoded))
	oldEncodedLen := zl.entrySize(offset)
	if encodedLen > oldEncodedLen {
		zl.expand(offset, encodedLen-oldEncodedLen)
	} else if encodedLen < oldEncodedLen {
		zl.shrink(offset+(encodedLen-oldEncodedLen), offset)
	}
	zl.write(offset, encoded)
}

// Push appends an entry to ziplist.
func (zl *Ziplist) Push(entry []byte) {
	endOffset := zl.Bytes() - 1
	zl.insert(endOffset, entry)
}

// PushLeft inserts an entry at the header of ziplist.
func (zl *Ziplist) PushLeft(entry []byte) {
	offset := ZiplistHeaderSize
	zl.insert(offset, entry)
}

// InsertAt inserts an entry at a specific position which locate with offset.
func (zl *Ziplist) InsertAt(offset uint32, content []byte) {
	zl.insert(offset, content)
}

func (zl *Ziplist) insert(offset uint32, content []byte) {
	var prevLen, nextDiff uint32
	bytes := zl.Bytes()
	ln := zl.Len()

	firstByte := ([]byte)(*zl)[offset]
	if firstByte == ZiplistEnd && ln > 0 { // end
		prevLen = zl.entrySize(zl.TailOffset())
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
	if firstByte != ZiplistEnd && ln > 0 { // header
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
	} else if firstByte != ZiplistEnd && ln == 0 {
		zl.write(offset+entrySize, []byte{byte(entrySize)})
	}
	bytes += entrySize + nextDiff
	zl.write(bytes-1, []byte{ZiplistEnd})
	zl.SetLen(ln + 1)
	zl.SetBytes(bytes)
	return
}

func (zl *Ziplist) InsertEncoded(offset uint32,
	encoded []byte, ln uint16, headPrevLen, tailLen uint32) {
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
	zl.addLen(ln)
	zl.addTailOffset(encodedLen)
	zl.write(zl.Bytes()-1, []byte{ZiplistEnd})
}

func (zl *Ziplist) ExtractEncoded() ([]byte, uint16, uint32, uint32) {
	head, tail := zl.HeadOffset(), zl.TailOffset()
	end := zl.Bytes()
	encoded := []byte(*zl)[head:end]
	ln := zl.Len()
	headPrevLen := zl.PrevLen(head)
	tailLen := zl.entrySize(tail)
	return encoded, ln, headPrevLen, tailLen
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

// Pop pops an entry from ziplist.
func (zl *Ziplist) Pop() []byte {
	removes, _, _ := zl.RemoveTail(1, 0)
	if removes != nil && len(removes) > 0 {
		return removes[0]
	}
	return nil
}

// PopLeft removes an entry at header of ziplist.
func (zl *Ziplist) PopLeft() []byte {
	removes, _, _ := zl.RemoveHead(1, 0)
	if removes != nil && len(removes) > 0 {
		return removes[0]
	}
	return nil
}

// RemoveHead removes entries from the header of ziplist, and skipnum entries can be skipped.
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
		offset += zl.entrySize(offset)
	}
	return offset
}

// RemoveTail removes entries from the tail of ziplist, and skipnum entries can be skipped.
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
	start += zl.entrySize(start)
	zl.shrink(offset, start)
	zl.addBytes(-(start - offset))
	zl.addTailOffset(-pprevlen)
	zl.addLen(-removedNum)
	return removes, skipnum, pass
}

func (zl *Ziplist) RemoveFromPos(offset uint32, num uint16) [][]byte {
	var (
		left, right = offset, uint32(0)
		removed     = make([][]byte, 0)
		i           = uint16(0)
	)
	for i = 0; i < num; i++ {
		if zl.atEnd(offset) {
			break
		}
		// The size is length of entry, not only the length of entry's content.
		entry, size := zl.DecodeEntry(offset)
		b := make([]byte, len(entry))
		copy(b, entry)
		removed = append(removed, b)
		offset += size
	}
	right = offset
	zl.shrink(left, right)
	zl.addBytes(-(right - left))
	zl.addTailOffset(-(right - left))
	zl.addLen(-i)
	return removed
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
	for entry := range zl.Iter() {
		nentry := make([]byte, len(entry.Content))
		copy(nentry, entry.Content)
		entries = append(entries, nentry)
	}
	return entries
}

func (zl *Ziplist) atEnd(offset uint32) bool {
	return []byte(*zl)[offset] == 255
}

// shrink removes entries which offset is between start and end
func (zl *Ziplist) shrink(start, end uint32) {
	(*zl) = append((*zl)[:start], (*zl)[end:]...)
}

// EntryAtIndex returns the entry at idx.
func (zl *Ziplist) EntryAtIndex(idx uint16) ([]byte, bool) {
	var entry []byte
	for entry := range zl.Iter() {
		if idx == 0 {
			return entry.Content, true
		}
		idx--
	}
	if idx >= 0 {
		return nil, false
	}
	return entry, true
}

func (zl *Ziplist) visualize() {
	fmt.Printf("Ziplist length: %d\n", zl.Len())
	fmt.Println("=============================")
	idx := 0
	for entry := range zl.Iter() {
		fmt.Printf("[%d] %s\n", idx, string(entry.Content))
		idx++
	}
}

// Find determines whether the entry exists. the index and the offset will be returned if exists.
func (zl *Ziplist) Find(ele []byte) (uint16, uint32, bool) {
	for entry := range zl.Iter() {
		if slices.Compare(ele, entry.Content) == 0 {
			return entry.Index, entry.Offset, true
		}
	}
	return 0, 0, false
}

type Entry struct {
	Index   uint16
	Offset  uint32
	Content []byte
	Options map[string]any
}

func (zl *Ziplist) Iter() <-chan Entry {
	ch := make(chan Entry)
	go func() {
		defer close(ch)
		idx, offset := uint16(0), uint32(ZiplistHeaderSize)
		for !zl.atEnd(offset) {
			entry, size := zl.DecodeEntry(offset)
			ch <- Entry{idx, offset, entry, nil}
			idx++
			offset += size
		}
	}()
	return ch
}
