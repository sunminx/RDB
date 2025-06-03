package list

import (
	"math"

	ds "github.com/sunminx/RDB/internal/datastruct"
	. "github.com/sunminx/RDB/pkg/util"
)

type Quicklist struct {
	head *QuicklistNode
	tail *QuicklistNode
	cnt  uint64 // total count of all entries in all ziplists
	ln   uint64 // number of quicklistNodes
}

func NewQuicklist() *Quicklist {
	return &Quicklist{}
}

func (ql *Quicklist) deepcopy() *Quicklist {
	var head, prev, cur *QuicklistNode
	nql := Quicklist{}
	node := ql.head
	for node != nil {
		cur := node.deepcopy()
		if head == nil {
			head = cur
		}
		if prev != nil {
			prev.next = cur
			cur.prev = prev
		}
		prev = cur
		node = node.next
	}
	nql.head = head
	nql.tail = cur
	nql.cnt = ql.cnt
	nql.ln = ql.ln
	return &nql
}

func (ql *Quicklist) Len() uint64 {
	return ql.ln
}

func (ql *Quicklist) Cnt() uint64 {
	return ql.cnt
}

func (ql *Quicklist) Head() *QuicklistNode {
	return ql.head
}

const (
	quicklistHead = 0
	quicklistTail = 1
)

func (ql *Quicklist) ReplaceAtIndex(index uint64, entry []byte) {
	if index < 0 {
		index = 0
	}
	if index >= ql.cnt {
		index = ql.cnt - 1
	}

	node := ql.head
	for {
		var step = Cond(index > math.MaxInt16, math.MaxInt16, index)
		if uint16(step) < node.cnt {
			break
		}
		index -= step
		node = node.next
	}
	node.zl.ReplaceAtIndex(uint16(index), entry)
	return
}

func (ql *Quicklist) PushLeft(entry []byte) {
	ql.insert(entry, quicklistHead)
	return
}

func (ql *Quicklist) Push(entry []byte) {
	ql.insert(entry, quicklistTail)
	return
}

func (ql *Quicklist) insert(entry []byte, where int8) {
	node := ql.getNodeOrCreateIfNeeded(uint32(len(entry)), where)
	node.insert(entry, where)
	ql.cnt++
	return
}

func (ql *Quicklist) getNodeOrCreateIfNeeded(entrylen uint32, where int8) *QuicklistNode {
	var node *QuicklistNode
	if ql.ln == 0 {
		node = newQuicklistNode()
		ql.ln++
		ql.head = node
		ql.tail = node
		return node
	}

	if where == quicklistHead {
		node = ql.head
		if node == nil || !node.insertAllowed(entrylen) {
			node = newQuicklistNode()
			if ql.head == nil {
				ql.head = node
			} else {
				head := ql.head
				ql.head = node
				head.prev = node
				node.next = head
			}
			ql.ln++
		}
	} else if where == quicklistTail {
		node = ql.tail
		if node == nil || !node.insertAllowed(entrylen) {
			node = newQuicklistNode()
			if ql.tail == nil {
				ql.tail = node
			} else {
				tail := ql.tail
				ql.tail = node
				node.prev = tail
				tail.next = node
			}
			ql.ln++
		}
	}
	return node
}

func (ql *Quicklist) Link(node *QuicklistNode) {
	if ql.tail == nil {
		ql.tail = node
	} else {
		tail := ql.tail
		ql.tail = node
		tail.next = node
		node.prev = tail
	}
	if ql.head == nil {
		ql.head = node
	}
	ql.ln += 1
	ql.cnt += uint64(node.cnt)
}

type QuicklistNode struct {
	prev    *QuicklistNode
	next    *QuicklistNode
	zl      *ds.Ziplist
	zlbytes uint32 // ziplist size in bytes
	cnt     uint16 // cnt of items in ziplist
	fill    uint16
}

func newQuicklistNode() *QuicklistNode {
	zl := ds.NewZiplist()
	return &QuicklistNode{
		zl:      zl,
		zlbytes: zl.Zlbytes(),
		cnt:     zl.Zllen(),
		fill:    2,
	}
}

func CreateQuicklistNode(zl *ds.Ziplist) *QuicklistNode {
	node := newQuicklistNode()
	node.zl = zl
	node.zlbytes = zl.Zlbytes()
	node.cnt = zl.Zllen()
	return node
}

func (n *QuicklistNode) deepcopy() *QuicklistNode {
	zl := n.zl
	nzl := zl.DeepCopy()
	return CreateQuicklistNode(nzl)
}

func (n *QuicklistNode) List() *ds.Ziplist {
	return n.zl
}

func (n *QuicklistNode) Next() *QuicklistNode {
	return n.next
}

func (n *QuicklistNode) insert(entry []byte, where int8) {
	if where == quicklistHead {
		n.zl.PushLeft(entry)
	} else if where == quicklistTail {
		n.zl.Push(entry)
	}
	n.zlbytes = n.zl.Zlbytes()
	n.cnt = n.zl.Zllen()
	return
}

func (n *QuicklistNode) insertAllowed(_len uint32) bool {
	elen := ds.ZiplistEntryEncodeLen(_len)
	nzlbytes := n.zlbytes + elen
	return quicklistNodeMeetsOptimizationLevel(nzlbytes, n.fill)
}

func (n *QuicklistNode) mergeNeeded(neighborNode *QuicklistNode) bool {
	if neighborNode == nil {
		return false
	}

	mergeLen := n.zlbytes + neighborNode.zlbytes - 11
	if quicklistNodeMeetsOptimizationLevel(mergeLen, n.fill) {
		return true
	} else if !quicklistNodeMeetsSafetyLimit(mergeLen) {
		return false
	} else if n.cnt+neighborNode.cnt < n.fill {
		return true
	} else {
		return false
	}
}

var optimizationLevel = []uint32{4096, 8192, 16384, 32768, 65536}

// var optimizationLevel = []int32{16, 64, 128, 512, 1024}

func quicklistNodeMeetsOptimizationLevel(_len uint32, fill uint16) bool {
	return _len < optimizationLevel[fill]
}

const safetyLimit = uint32(8192)

// const safetyLimit int32 = 64

func quicklistNodeMeetsSafetyLimit(_len uint32) bool {
	return _len < safetyLimit
}

func (n *QuicklistNode) insertEncoded(offset uint32, encoded []byte, _len uint16, headprevlen, tailLen uint32) {
	n.zl.InsertEncoded(offset, encoded, _len, headprevlen, tailLen)
}

func (n *QuicklistNode) extractEncoded() ([]byte, uint16, uint32, uint32) {
	return n.zl.ExtractEncoded()
}

func (n *QuicklistNode) headOffset() uint32 {
	return n.zl.Zlhead()
}

func (n *QuicklistNode) endOffset() uint32 {
	return n.zl.Zlbytes()
}

func (ql *Quicklist) PopLeft() [][]byte {
	return ql.remove(quicklistHead, 1, 0)
}

func (ql *Quicklist) Pop() [][]byte {
	return ql.remove(quicklistTail, 1, 0)
}

func (ql *Quicklist) remove(where int8, num, skipnum uint64) [][]byte {
	if ql.cnt == 0 {
		return nil
	}
	var node, neighborNode *QuicklistNode
	var removenum, skipenum, skipednum uint16
	var pass bool
	if where == quicklistHead {
		node = ql.head
	} else if where == quicklistTail {
		node = ql.tail
	}
	removeSlice := make([][]byte, 0)
	for num > 0 {
		var removes [][]byte
		removenum = uint16(Cond(num > math.MaxInt16, math.MaxInt16, num))
		skipenum = uint16(Cond(skipnum > math.MaxInt16, math.MaxInt16, skipnum))
		if where == quicklistHead {
			neighborNode = node.next
			removes, skipednum, pass = node.zl.RemoveHead(removenum, skipenum)
			if pass {
				if skipednum == 0 {
					ql.ln--
					ql.head = neighborNode
					node = ql.head
				} else {
					node = neighborNode
				}
			}
		} else if where == quicklistTail {
			neighborNode = node.prev
			removes, skipednum, pass = node.zl.RemoveTail(removenum, skipenum)
			if pass {
				if skipednum == 0 {
					ql.ln--
					ql.tail = neighborNode
				} else {
					node = neighborNode
				}
			}
		}
		removedNum := uint64(0)
		if removes != nil {
			removedNum = uint64(len(removes))
			removeSlice = append(removeSlice, removes...)
		}
		num -= removedNum
		skipnum -= uint64(skipednum)
		ql.cnt -= removedNum
		if neighborNode == nil {
			break
		}
	}

	if node.mergeNeeded(neighborNode) {
		if where == quicklistHead {
			node = ql.unlinkHeadNode()
			offset := node.headOffset()
			encoded, ln, headprevlen, taillen := node.extractEncoded()
			ql.head.insertEncoded(offset, encoded, ln, headprevlen, taillen)
		} else if where == quicklistTail {
			node = ql.unlinkTailNode()
			offset := node.endOffset()
			encoded, ln, headprevlen, taillen := node.extractEncoded()
			ql.tail.insertEncoded(offset, encoded, ln, headprevlen, taillen)
		}
	}
	return removeSlice
}

func (ql *Quicklist) unlinkHeadNode() *QuicklistNode {
	node := ql.head
	if node != nil {
		ql.head = node.next
	}
	return node
}

func (ql *Quicklist) unlinkTailNode() *QuicklistNode {
	node := ql.tail
	if node != nil {
		ql.tail = node.prev
	}
	return node
}

func (ql *Quicklist) Index(idx uint64) ([]byte, bool) {
	if idx >= ql.cnt {
		return nil, false
	}
	var entry []byte
	iter := newQuicklistIterator(ql)
	for iter.HasNext() {
		entry = iter.next()
		if idx == 0 {
			break
		}
		idx--
	}
	return entry, true
}

func (ql *Quicklist) Range(start, end uint64) (entrys [][]byte) {
	if start >= end {
		return
	}
	if start < 0 {
		start = 0
	}
	if end > ql.cnt {
		end = ql.cnt
	}

	var entry []byte
	iter := newQuicklistIterator(ql)
	for iter.HasNext() {
		entry = iter.next()
		if start < iter.idx {
			entrys = append(entrys, entry)
		}
		if end <= iter.idx {
			break
		}
	}
	return
}

func (ql *Quicklist) Trim(start, end uint64) {
	if start >= end {
		return
	}
	if start < 0 {
		start = 0
	}
	if end > ql.cnt {
		end = ql.cnt
	}

	removenum := start
	ql.remove(quicklistHead, removenum, 0)
	removenum = ql.cnt - end
	ql.remove(quicklistTail, removenum, 0)
	return
}

type quicklistNodeIterator struct {
	*ds.ZiplistIterator
}

func newQuicklistNodeIterator(node *QuicklistNode) *quicklistNodeIterator {
	return &quicklistNodeIterator{
		ZiplistIterator: ds.NewZiplistIterator(node.zl),
	}
}

type quicklistIterator struct {
	list     *Quicklist
	node     *QuicklistNode
	nodeIter *quicklistNodeIterator
	idx      uint64
}

func newQuicklistIterator(list *Quicklist) *quicklistIterator {
	node := list.head
	return &quicklistIterator{
		list:     list,
		node:     node,
		nodeIter: newQuicklistNodeIterator(node),
		idx:      0,
	}
}

func (iter *quicklistIterator) HasNext() bool {
	return iter.idx < iter.list.cnt
}

func (iter *quicklistIterator) Next() any {
	return iter.next()
}

func (iter *quicklistIterator) next() []byte {
	if !iter.nodeIter.HasNext() {
		iter.node = iter.node.next
		iter.nodeIter = newQuicklistNodeIterator(iter.node)
	}
	iter.idx++
	return iter.nodeIter.Next()
}
