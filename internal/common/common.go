package common

type Reply struct {
	RespErrWrongType    []byte
	RespErrInvalidIndex []byte
	RespStrOK           []byte
	RespIntZero         []byte
	RespIntOne          []byte
	RespBulkNull        []byte
	RespBulkEmpty       []byte
}

var Shared *Reply = &Reply{
	RespErrWrongType:    []byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
	RespErrInvalidIndex: []byte("-ERR invalid index value\r\n"),
	RespStrOK:           []byte("+OK\r\n"),
	RespIntZero:         []byte(":0\r\n"),
	RespIntOne:          []byte(":1\r\n"),
	RespBulkNull:        []byte("$-1\r\n"),
	RespBulkEmpty:       []byte("$0\r\n\r\n"),
}
