//go:build debug
// +build debug

package debug

func Assert(condition bool, message string) {
	if condition {
		panic(message)
	}
}
