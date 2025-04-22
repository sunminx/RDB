package list

type quicklist struct {
	head  *quicklistNode
	tail  *quicklistNode
	count int64 // total count of all entries in all ziplists
	_len  int64 // number of quicklistNodes
}

type quicklistNode struct {
	prev    *quicklistNode
	next    *quicklistNode
	zl      *ziplist
	zlbytes int32 // ziplist size in bytes
	count   int16 // count of items in ziplist
}

func NewQuicklist() quicklist {
	return quicklist{
		count: 0,
		_len:  0,
	}
}

func newQuicklistNode() *quicklistNode {
	zl := NewZiplist()
	return &quicklistNode{
		zl:      zl,
		zlbytes: zl.zlbytes(),
		count:   zl.zllen(),
	}
}

func (l *quicklist) PushLeft(entry []byte) {
	return l.insert(entry, quicklistHead)
}

func (l *quicklist) Push(entry []byte) {
	return l.insert(entry, quicklistTail)
}

const (
	quicklistHead = 0
	quicklistTail = 0
)

func (l *quicklist) insert(entry []byte, where int8) {
	node := l.getNodeOrCreateIfNeeded(where)
	inserted := node.insert(entry, where)
	return
}

func (l *quicklist) getNodeOrCreateIfNeeded(where int8) *quicklistNode {
	var node *quicklistNode
	if where == quicklistHead {
		node = l.head
		if node == nil || !node.insertAllowed() {
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
		if node == nil || !node.insertAllowed {
			node = newQuicklistNode()
			if l.tail == nil {
				l.tail = node
			} else {
				tail := l.tail
				t.tail = node
				node.prev = tail
				tail.next = node
			}
			l._len++
		}
	}
	if l._len == 1 {
		if l.head == nil {
			l.head = l.tail
		}
		if l.tail == nil {
			l.tail = l.head
		}
	}
	return quicklistNode
}

func (n *quicklistNode) insert(entry []byte, where int8) {
	if where == quicklistHead {
		n.zl.PushLeft(entry)
	} else if where == quickTail {
		n.zl.Push(entry)
	}
	n.zlbytes = n.zl.zlbytes()
	n.zl = n.zl.zllen()
	return
}

func (n *quicklistNode) insertAllowed() bool {
	return true
}

func (n *quicklistNode) mergeAllowed() bool {

}

func (l *quicklist) Pop() {

}

func (l *quicklist) PopLeft() {

}

func (l *quicklist) remove(num int32, where int8) int32 {
	if l.count == 0 {
		return
	}
	var node *quicklistNode
	if where == quicklistHead {
		node = l.head
		node.zl.remove()
	} else if where == quicklistTail {
		node = l.tail
		node.zl.remove()
	}

	if node.mergeAllowed() {

	}
	return
}
