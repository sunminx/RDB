package resp

import (
	"errors"
	"slices"

	"github.com/sunminx/RDB/internal/sds"
)

type ProtoReqType int

const (
	Inline ProtoReqType = iota + 1
	MultiBulk
)

func Decode(data *[]byte) ([]*sds.SDS, error) {
	if len(*data) < 1 {
		return nil, errors.New("invalid bytes")
	}

	if (*data)[0] == '*' {
		return decodeMultibulk(data)
	}
	return decodeInline(data)

}

func decodeInline(data *[]byte) ([]*sds.SDS, error) {
	idx := slices.Index(*data, '\n')
	if idx < 0 {
		return nil, errors.New("")
	}

	gap := 0
	if (*data)[idx-1] == '\r' {
		idx -= 1
		gap += 1
	}
	newline := (*data)[:idx]
	*data = (*data)[idx+gap:]
	return splitByteSlice(newline, ' '), nil
}

func splitByteSlice(bytes []byte, sep byte) []*sds.SDS {
	res := make([]*sds.SDS, 0)
	i := 0
	for j := 0; j < len(bytes); j++ {
		if bytes[j] == sep {
			res = append(res, sds.New(bytes[i:j]))
			i = j
		}
	}
	res = append(res, sds.New(bytes[i:]))
	return res
}

func decodeMultibulk(data *[]byte) ([]*sds.SDS, error) {
	return nil, nil
}
