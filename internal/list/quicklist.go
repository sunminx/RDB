package list

import (
	"math"

	"github.com/sunminx/RDB/pkg/util"
)

type Quicklist struct {
	head  *quicklistNode
	tail  *quicklistNode
	count int64 // total count of all entries in all ziplists
	_len  int64 // number of quicklistNodes
}

func NewQuicklist() *Quicklist {
	return &Quicklist{}
}

func (l *Quicklist) Len() int64 {
	return l.count
}

const (
	quicklistHead = 0
	quicklistTail = 1
)

func (l *Quicklist) ReplaceAtIndex(index int64, entry []byte) {
	if index < 0 {
		index = 0
	}
	if index >= l.count {
		index = l.count - 1
	}

	node := l.head
	for {
		var step int64 = util.CondInt64(index > math.MaxInt16, math.MaxInt16, index)
		if int16(step) < node.count {
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
	l.count++
	return
}

func (l *Quicklist) getNodeOrCreateIfNeeded(entrylen int32,
	where int8) *quicklistNode {

	var node *quicklistNode
	if l._len == 0 {
		node = newQuicklistNode()
		l._len++
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
			l._len++
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
			l._len++
		}
	}
	return node
}

type quicklistNode struct {
	prev    *quicklistNode
	next    *quicklistNode
	zl      *ziplist
	zlbytes int32 // ziplist size in bytes
	count   int16 // count of items in ziplist
	fill    int16
}

func newQuicklistNode() *quicklistNode {
	zl := NewZiplist()
	return &quicklistNode{
		zl:      zl,
		zlbytes: zl.zlbytes(),
		count:   zl.zllen(),
		fill:    2,
	}
}

func (n *quicklistNode) insert(entry []byte, where int8) {
	if where == quicklistHead {
		n.zl.PushLeft(entry)
	} else if where == quicklistTail {
		n.zl.Push(entry)
	}
	n.zlbytes = n.zl.zlbytes()
	n.count = n.zl.zllen()
	return
}

func (n *quicklistNode) insertAllowed(_len int32) bool {
	elen := entryEncodeLen(_len)
	nzlbytes := n.zlbytes + elen
	return quicklistNodeMeetsOptimizationLevel(nzlbytes, n.fill)
}

func (n *quicklistNode) mergeNeeded(neighborNode *quicklistNode) bool {
	if neighborNode == nil {
		return false
	}

	mergeLen := n.zlbytes + neighborNode.zlbytes - 11
	if quicklistNodeMeetsOptimizationLevel(mergeLen, n.fill) {
		return true
	} else if !quicklistNodeMeetsSafetyLimit(mergeLen) {
		return false
	} else if n.count+neighborNode.count < n.fill {
		return true
	} else {
		return false
	}
}

// var optimizationLevel = []int32{4096, 8192, 16384, 32768, 65536}
var optimizationLevel = []int32{16, 64, 128, 512, 1024}

func quicklistNodeMeetsOptimizationLevel(_len int32, fill int16) bool {
	return _len < optimizationLevel[fill]
}

// const safetyLimit int32 = 8192
const safetyLimit int32 = 64

func quicklistNodeMeetsSafetyLimit(_len int32) bool {
	return _len < safetyLimit
}

func (n *quicklistNode) insertEncoded(offset int32, encoded []byte,
	_len int16, headprevlen, taillen int32) {
	n.zl.insertEncoded(offset, encoded, _len, headprevlen, taillen)
	return
}

func (n *quicklistNode) extractEncoded() (encoded []byte,
	_len int16, headprevlen, taillen int32) {
	return n.zl.extractEncoded()
}

func (n *quicklistNode) headOffset() int32 {
	return n.zl.zlhead()
}

func (n *quicklistNode) endOffset() int32 {
	return n.zl.zlbytes()
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
	if l.count == 0 {
		return 0
	}

	var node, neighborNode *quicklistNode
	var removenum, skipenum, removednum, skipednum int16
	var pass bool
	if where == quicklistHead {
		node = l.head
	} else if where == quicklistTail {
		node = l.tail
	}
	for num > 0 {
		removenum = int16(util.CondInt64(num > math.MaxInt16, math.MaxInt16, num))
		skipenum = int16(util.CondInt64(skipnum > math.MaxInt16, math.MaxInt16, skipnum))
		if where == quicklistHead {
			neighborNode = node.next
			removednum, skipednum, pass = node.zl.removeHead(removenum, skipenum)
			if pass {
				if l.head.zl.zllen() == 0 {
					l._len--
					l.head = neighborNode
					node = l.head
				} else {
					node = neighborNode
				}
			}
		} else if where == quicklistTail {
			neighborNode = node.prev
			removednum, skipednum, pass = node.zl.removeTail(removenum, skipenum)
			if pass {
				if l.tail.zl.zllen() == 0 {
					l._len--
					l.tail = neighborNode
				} else {
					node = neighborNode
				}
			}
		}
		num -= int64(removednum)
		skipnum -= int64(skipednum)
		l.count -= int64(removednum)
		if neighborNode == nil {
			break
		}
	}

	if node.mergeNeeded(neighborNode) {
		if where == quicklistHead {
			node = l.unlinkHeadNode()
			offset := node.headOffset()
			encoded, _len, headprevlen, taillen := node.extractEncoded()
			l.head.insertEncoded(offset, encoded, _len, headprevlen, taillen)
		} else if where == quicklistTail {
			node = l.unlinkTailNode()
			offset := node.endOffset()
			encoded, _len, headprevlen, taillen := node.extractEncoded()
			l.tail.insertEncoded(offset, encoded, _len, headprevlen, taillen)
		}
	}
	return 0
}

func (l *Quicklist) unlinkHeadNode() *quicklistNode {
	node := l.head
	if node != nil {
		l.head = node.next
	}
	return node
}

func (l *Quicklist) unlinkTailNode() *quicklistNode {
	node := l.tail
	if node != nil {
		l.tail = node.prev
	}
	return node
}

func (l *Quicklist) Index(idx int64) ([]byte, bool) {
	if idx >= l.count {
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
	if end > l.count {
		end = l.count
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
	if end > l.count {
		end = l.count
	}

	var removenum int64
	removenum = start
	l.remove(quicklistHead, removenum, 0)
	removenum = l.count - end
	l.remove(quicklistTail, removenum, 0)
	return
}

type quicklistNodeIterator struct {
	*ziplistIterator
}

func newQuicklistNodeIterator(node *quicklistNode) *quicklistNodeIterator {
	return &quicklistNodeIterator{
		ziplistIterator: newZiplistIterator(node.zl),
	}
}

type quicklistIterator struct {
	list     *Quicklist
	node     *quicklistNode
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
	return iter.idx < iter.list.count
}

func (iter *quicklistIterator) next() []byte {
	if !iter.nodeIter.hasNext() {
		iter.node = iter.node.next
		iter.nodeIter = newQuicklistNodeIterator(iter.node)
	}
	iter.idx++
	return iter.nodeIter.next()
}
