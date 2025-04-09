package sds

import (
	"slices"
	"testing"
)

func TestNew(t *testing.T) {
	testcases := []struct {
		input []byte
	}{
		{[]byte("")},
		{[]byte(" ")},
		{[]byte("redis")},
		{[]byte("hello redis")},
	}

	for _, tc := range testcases {
		output := New(tc.input)
		if !slices.Equal(output.Bytes(), tc.input) {
			t.Error("sds New failed")
		}
	}
}

func TestDup(t *testing.T) {
	testcases := []struct {
		input []byte
	}{
		{[]byte("")},
		{[]byte(" ")},
		{[]byte("redis")},
		{[]byte("hello redis")},
	}

	for _, tc := range testcases {
		var sds = New(tc.input)
		output := sds.Dup()
		if !slices.Equal((output.Bytes()),
			sds.Bytes()) {
			t.Error("sds Dup failed")
		}
	}
}

func TestEmpty(t *testing.T) {
	testcases := []struct {
		input []byte
	}{
		{[]byte("")},
		{[]byte(" ")},
		{[]byte("redis")},
		{[]byte("hello redis")},
	}

	for _, tc := range testcases {
		var sds = New(tc.input)
		sds.Empty()
		if !slices.Equal((sds.Bytes()),
			([]byte(""))) {
			t.Error("sds Empty failed")
		}
	}
}

func TestCat(t *testing.T) {
	testcases := []struct {
		dst, str, want string
	}{
		{dst: "", str: "", want: ""},
		{dst: " ", str: "", want: " "},
		{dst: "", str: " ", want: " "},
		{dst: "hello", str: "", want: "hello"},
		{dst: "", str: "redis", want: "redis"},
		{dst: "hello", str: "redis", want: "helloredis"},
		{dst: "hello ", str: "redis", want: "hello redis"},
		{dst: "hello", str: " redis", want: "hello redis"},
	}

	for _, tc := range testcases {
		var s1 = New([]byte(tc.dst))
		var s2 = New([]byte(tc.str))
		s1.Cat(s2)
		if string(s1.Bytes()) != tc.want {
			t.Error("sds Cat failed")
		}
	}
}

func TestCmp(t *testing.T) {
	testcases := []struct {
		dst, str string
		want     int
	}{
		{dst: "", str: "", want: 0},
		{dst: " ", str: "", want: 1},
		{dst: "", str: " ", want: -1},
	}

	for _, tc := range testcases {
		var s1 = New([]byte(tc.dst))
		var s2 = New([]byte(tc.str))
		if s1.Cmp(s2) != tc.want {
			t.Error("sds Cmp failed")
		}
	}
}

func TestJoin(t *testing.T) {
	testcases := []struct {
		strs      []string
		sep, want string
	}{
		{strs: []string{"", ""}, want: ",", sep: ","},
		{strs: []string{" ", ""}, want: " ,", sep: ","},
		{strs: []string{"", " "}, want: ", ", sep: ","},
		{strs: []string{"hello ", "redis"}, want: "hello ,redis", sep: ","},
	}

	for _, tc := range testcases {
		var s = Join(tc.strs, tc.sep)
		if string(s.Bytes()) != tc.want {
			t.Log("sds Join failed")
		}
	}
}

func TestCpy(t *testing.T) {
	testcases := []struct {
		dst, str string
	}{
		{dst: "", str: ""},
		{dst: " ", str: ""},
		{dst: "he", str: "redis"},
		{dst: "redis", str: "he"},
	}

	for _, tc := range testcases {
		s := New([]byte(tc.dst))
		s.Cpy(tc.str)
		if string(s.Bytes()) != tc.str {
			t.Errorf("sds Cpy failed, want: %s", tc.str)
		}
	}
}

func TestEqual(t *testing.T) {
	testcases := []struct {
		in1, in2 []byte
		want     bool
	}{
		{in1: []byte(""), in2: []byte(""), want: true},
		{in1: []byte(""), in2: []byte(" "), want: false},
		{in1: []byte("redis"), in2: []byte("reisd"), want: false},
	}

	for _, tc := range testcases {
		s1 := New(tc.in1)
		s2 := New(tc.in2)
		output := s1.Equal(s2)
		if output != tc.want {
			t.Error("sds Equal failed")
		}

	}
}
