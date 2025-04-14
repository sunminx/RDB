package common

var Shared map[string][]byte = map[string][]byte{
	"wrongtypeerr": []byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
	"crlf":         []byte("\r\n"),
	"ok":           []byte("OK"),
}
