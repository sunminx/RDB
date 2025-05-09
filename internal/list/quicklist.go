package list

import (
	"math"

	ds "github.com/sunminx/RDB/internal/datastruct"
	"github.com/sunminx/RDB/pkg/util"
)

type Quicklist struct {
	head *QuicklistNode
	tail *QuicklistNode
	cnt  int64 // total count of all entries in all ziplists
	ln   int64 // number of quicklistNodes
}

func NewQuicklist() *Quicklist {
	return &Quicklist{}
}

func (l *Quicklist) Len() int64 {
	return l.cnt
}

func (l *Quicklist) Head() *QuicklistNode {
	return l.head
}

const (
	quicklistHead = 0
	quicklistTail = 1
)

func (l *Quicklist) ReplaceAtIndex(index int64, entry []byte) {
	if index < 0 {
		index = 0
	}
	if index >= l.cnt {
		index = l.cnt - 1
	}

	node := l.head
	for {
		var step int64 = util.Cond(index > math.MaxInt16, math.MaxInt16, index)
		if int16(step) < node.cnt {
			break
		}
		index -= step
		node = node.next
	}
	node.zl.ReplaceAtIndex(int16(index), entry)
	return
}

func (l *Quicklist) PushLeft(entry []byte) {
	l.insert(entry, quicklistHead)
	return
}

func (l *Quicklist) Push(entry []byte) {
	l.insert(entry, quicklistTail)
	return
}

func (l *Quicklist) insert(entry []byte, where int8) {
	node := l.getNodeOrCreateIfNeeded(int32(len(entry)), where)
	node.insert(entry, where)
	l.cnt++
	return
}

func (l *Quicklist) getNodeOrCreateIfNeeded(entrylen int32,
	where int8) *QuicklistNode {

	var node *QuicklistNode
	if l.ln == 0 {
		node = newQuicklistNode()
		l.ln++
		l.head = node
		l.tail = node
		return node
	}

	if where == quicklistHead {
		node = l.head
		if node == nil || !node.insertAllowed(entrylen) {
			node = newQuicklistNode()
			if l.head == nil {
				l.head = node
			} else {
				head := l.head
				l.head = node
				head.prev = node
				node.next = head
			}
			l.ln++
		}
	} else if where == quicklistTail {
		node = l.tail
		if node == nil || !node.insertAllowed(entrylen) {
			node = newQuicklistNode()
			if l.tail == nil {
				l.tail = node
			} else {
				tail := l.tail
				l.tail = node
				node.prev = tail
				tail.next = node
			}
			l.ln++
		}
	}
	return node
}

func (l *Quicklist) Link(node *QuicklistNode) {
	if l.tail == nil {
		l.tail = node
	} else {
		tail := l.tail
		l.tail = node
		tail.next = node
		node.prev = tail
	}
	if l.head == nil {
		l.head = node
	}
	l.ln += 1
	l.cnt += int64(node.cnt)
}

type QuicklistNode struct {
	prev    *QuicklistNode
	next    *QuicklistNode
	zl      *ds.Ziplist
	zlbytes int32 // ziplist size in bytes
	cnt     int16 // cnt of items in ziplist
	fill    int16
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

func (n *QuicklistNode) insertAllowed(ln int32) bool {
	elen := ds.ZiplistEntryEncodeLen(ln)
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

// var optimizationLevel = []int32{4096, 8192, 16384, 32768, 65536}
var optimizationLevel = []int32{16, 64, 128, 512, 1024}

func quicklistNodeMeetsOptimizationLevel(ln int32, fill int16) bool {
	return ln < optimizationLevel[fill]
}

// const safetyLimit int32 = 8192
const safetyLimit int32 = 64

func quicklistNodeMeetsSafetyLimit(ln int32) bool {
	return ln < safetyLimit
}

func (n *QuicklistNode) insertEncoded(offset int32, encoded []byte,
	ln int16, headprevlen, taillen int32) {
	n.zl.InsertEncoded(offset, encoded, ln, headprevlen, taillen)
	return
}

func (n *QuicklistNode) extractEncoded() (encoded []byte,
	ln int16, headprevlen, taillen int32) {
	return n.zl.ExtractEncoded()
}

func (n *QuicklistNode) headOffset() int32 {
	return n.zl.Zlhead()
}

func (n *QuicklistNode) endOffset() int32 {
	return n.zl.Zlbytes()
}

func (l *Quicklist) PopLeft() {
	l.remove(quicklistHead, 1, 0)
	return
}

func (l *Quicklist) Pop() {
	l.remove(quicklistTail, 1, 0)
	return
}

func (l *Quicklist) remove(where int8, num, skipnum int64) int64 {
	if l.cnt == 0 {
		return 0
	}

	var node, neighborNode *QuicklistNode
	var removenum, skipenum, removednum, skipednum int16
	var pass bool
	if where == quicklistHead {
		node = l.head
	} else if where == quicklistTail {
		node = l.tail
	}
	for num > 0 {
		removenum = int16(util.Cond(num > math.MaxInt16, math.MaxInt16, num))
		skipenum = int16(util.Cond(skipnum > math.MaxInt16, math.MaxInt16, skipnum))
		if where == quicklistHead {
			neighborNode = node.next
			removednum, skipednum, pass = node.zl.RemoveHead(removenum, skipenum)
			if pass {
				if l.head.zl.Zllen() == 0 {
					l.ln--
					l.head = neighborNode
					node = l.head
				} else {
					node = neighborNode
				}
			}
		} else if where == quicklistTail {
			neighborNode = node.prev
			removednum, skipednum, pass = node.zl.RemoveTail(removenum, skipenum)
			if pass {
				if l.tail.zl.Zllen() == 0 {
					l.ln--
					l.tail = neighborNode
				} else {
					node = neighborNode
				}
			}
		}
		num -= int64(removednum)
		skipnum -= int64(skipednum)
		l.cnt -= int64(removednum)
		if neighborNode == nil {
			break
		}
	}

	if node.mergeNeeded(neighborNode) {
		if where == quicklistHead {
			node = l.unlinkHeadNode()
			offset := node.headOffset()
			encoded, ln, headprevlen, taillen := node.extractEncoded()
			l.head.insertEncoded(offset, encoded, ln, headprevlen, taillen)
		} else if where == quicklistTail {
			node = l.unlinkTailNode()
			offset := node.endOffset()
			encoded, ln, headprevlen, taillen := node.extractEncoded()
			l.tail.insertEncoded(offset, encoded, ln, headprevlen, taillen)
		}
	}
	return 0
}

func (l *Quicklist) unlinkHeadNode() *QuicklistNode {
	node := l.head
	if node != nil {
		l.head = node.next
	}
	return node
}

func (l *Quicklist) unlinkTailNode() *QuicklistNode {
	node := l.tail
	if node != nil {
		l.tail = node.prev
	}
	return node
}

func (l *Quicklist) Index(idx int64) ([]byte, bool) {
	if idx >= l.cnt {
		return nil, false
	}

	var entry []byte
	iter := newQuicklistIterator(l)
	for idx >= 0 && iter.hasNext() {
		entry = iter.next()
		idx--
	}
	return entry, true
}

func (l *Quicklist) Range(start, end int64) (entrys [][]byte) {
	if start >= end {
		return
	}
	if start < 0 {
		start = 0
	}
	if end > l.cnt {
		end = l.cnt
	}

	var entry []byte
	iter := newQuicklistIterator(l)
	for iter.hasNext() {
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

func (l *Quicklist) Trim(start, end int64) {
	if start >= end {
		return
	}
	if start < 0 {
		start = 0
	}
	if end > l.cnt {
		end = l.cnt
	}

	var removenum int64
	removenum = start
	l.remove(quicklistHead, removenum, 0)
	removenum = l.cnt - end
	l.remove(quicklistTail, removenum, 0)
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
	idx      int64
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

func (iter *quicklistIterator) hasNext() bool {
	return iter.idx < iter.list.cnt
}

func (iter *quicklistIterator) next() []byte {
	if !iter.nodeIter.HasNext() {
		iter.node = iter.node.next
		iter.nodeIter = newQuicklistNodeIterator(iter.node)
	}
	iter.idx++
	return iter.nodeIter.Next()
}
