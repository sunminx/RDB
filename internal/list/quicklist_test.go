package list

import (
	"strings"
	"testing"
)

func TestQuicklistPush(t *testing.T) {
	list := NewQuicklist()
	list.Push([]byte("hello"))
	list.Push([]byte("123456"))
	entry, ok := list.head.zl.Index(1)
	if ok {
		//t.Log(string(entry.([]byte)))
		t.Log(entry)
	} else {
		t.Log(ok)
	}
}

func TestQuicklistPushWithCreateNewNode(t *testing.T) {
	list := NewQuicklist()
	list.Push([]byte(strings.Repeat("aaaaa", 6)))
	list.Push([]byte(strings.Repeat("bbbbb", 6)))
	list.Push([]byte("123456"))
	list.Push([]byte(strings.Repeat("ccccc", 6)))
	list.Push([]byte(strings.Repeat("ddddd", 6)))
	list.Push([]byte(strings.Repeat("eeeee", 6)))

	for i := 0; i < 6; i++ {
		entry, ok := list.Index(int64(i))
		if ok {
			//t.Log(string(entry.([]byte)))
			t.Log(entry)
		}
	}
	t.Log(list._len)
}

func TestQuicklistPop(t *testing.T) {
	list := NewQuicklist()
	list.Push([]byte(strings.Repeat("aaaaa", 6)))
	list.Push([]byte("123456"))
	list.Push([]byte(strings.Repeat("ddddd", 6)))
	list.PopLeft()
	list.PopLeft()
	//list.Pop()
	//list.Pop()
	//list.Pop()
	//t.Log(list.count)
	for i := 0; i < 3; i++ {
		entry, ok := list.Index(int64(i))
		if ok {
			//t.Log(string(entry.([]byte)))
			t.Log(entry)
		}
	}
}
