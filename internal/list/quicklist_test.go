package list

import (
	"strings"
	"testing"
)

func TestQuicklistPush(t *testing.T) {
	list := NewQuicklist()
	list.Push([]byte("hello"))
	list.Push([]byte("123456"))
	entry, ok := list.Index(0)
	if ok {
		t.Log(entry)
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
	t.Log(list.ln)
}

func TestQuicklistPop(t *testing.T) {
	list := NewQuicklist()
	list.Push([]byte(strings.Repeat("aaaaa", 6)))
	list.Push([]byte(strings.Repeat("bbbbb", 6)))
	list.Push([]byte(strings.Repeat("ccccc", 6)))
	list.Push([]byte("123456"))
	list.Push([]byte(strings.Repeat("ddddd", 6)))
	list.Push([]byte(strings.Repeat("eeeee", 6)))
	list.Push([]byte(strings.Repeat("fffff", 6)))
	t.Log(list.cnt)
	list.PopLeft()
	t.Log(list.cnt)
	list.PopLeft()
	list.Pop()
	list.Pop()
	list.Pop()
	t.Log(list.cnt)
	for i := 0; i < 3; i++ {
		entry, ok := list.Index(int64(i))
		if ok {
			//t.Log(string(entry.([]byte)))
			t.Log(entry)
		}
	}
}

func TestQuicklistRemove(t *testing.T) {
	list := NewQuicklist()
	list.Push([]byte(strings.Repeat("aaaaa", 6)))
	list.Push([]byte(strings.Repeat("bbbbb", 6)))
	list.Push([]byte(strings.Repeat("ccccc", 6)))
	list.Push([]byte("123456"))
	list.Push([]byte(strings.Repeat("ddddd", 6)))
	list.Push([]byte(strings.Repeat("eeeee", 6)))
	list.Push([]byte(strings.Repeat("fffff", 6)))
	t.Log(list.cnt)
	t.Log(list.ln)
	list.remove(quicklistTail, 3, 3)
	t.Log(list.cnt)
	t.Log(list.ln)
	entry, _ := list.Index(1)
	t.Log(string(entry))
}

func TestQuicklistReplaceAtIndex(t *testing.T) {
	list := NewQuicklist()
	list.Push([]byte(strings.Repeat("aaaaa", 6)))
	list.Push([]byte(strings.Repeat("bbbbb", 6)))
	list.Push([]byte(strings.Repeat("ccccc", 6)))
	list.Push([]byte("123456"))
	list.Push([]byte(strings.Repeat("ddddd", 6)))
	list.Push([]byte(strings.Repeat("eeeee", 6)))
	list.Push([]byte(strings.Repeat("fffff", 6)))
	t.Log(list.cnt)
	t.Log(list.ln)
	list.ReplaceAtIndex(3, []byte(strings.Repeat("ggggg", 6)))
	entry, _ := list.Index(2)
	t.Log(string(entry))
}
