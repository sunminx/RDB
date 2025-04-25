package common

var Shared map[string][]byte = map[string][]byte{
	"wrongtypeerr": []byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
	"crlf":         []byte("\r\n"),
	"ok":           []byte("OK"),
	"czero":        []byte(":0\r\n"),
	"cone":         []byte(":1\r\n"),
	"nullbulk":     []byte("$-1\r\n"),
	"invalidindex": []byte("invalid index value"),
}
