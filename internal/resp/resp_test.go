package resp

import (
	"slices"
	"testing"
)

func TestSplitByteSlice(t *testing.T) {
	testcases := []struct {
		input []byte
		want  []byte
	}{
		{input: []byte("OK"), want: []byte("OK")},
	}

	for _, tc := range testcases {
		output := splitByteSlice(tc.input, ' ')
		if !slices.Equal(([]byte)(output[0]), tc.want) {
			t.Errorf("splitByteSlice, output: %s", string(output[0].Bytes()))
		}
	}
}

func TestDecodeInline(t *testing.T) {

}
