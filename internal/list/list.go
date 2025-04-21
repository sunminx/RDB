package list

type list interface {
	push(any)
	pop() any
	index(int) any
}
