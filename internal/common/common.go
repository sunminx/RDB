package common

type Reply struct {
	RespStrOK           []byte
	RespIntZero         []byte
	RespIntOne          []byte
	RespBulkNull        []byte
	RespBulkEmpty       []byte
	RespErrWrongType    []byte
	RespErrInvalidIndex []byte
	RespErrSyntax       []byte
}

var Shared *Reply = &Reply{
	RespStrOK:           []byte("+OK\r\n"),
	RespIntZero:         []byte(":0\r\n"),
	RespIntOne:          []byte(":1\r\n"),
	RespBulkNull:        []byte("$-1\r\n"),
	RespBulkEmpty:       []byte("$0\r\n\r\n"),
	RespErrWrongType:    []byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
	RespErrInvalidIndex: []byte("-ERR invalid index value\r\n"),
	RespErrSyntax:       []byte("-ERR syntax error\r\n"),
}
