package object

type RobjType int

const (
	UnknownType RobjType = iota
	TypeString
	TypeList
	TypeHash
)

type EncodingType int

const (
	UnknownEncodingType EncodingType = iota
	EncodingInt
	EncodingRaw
	EncodingZiplist
	EncodingQuicklist
	EncodingZipmap
)

type Robj struct {
	typ      RobjType
	encoding EncodingType
	val      any
}

func New(val any, typ RobjType, encoding EncodingType) *Robj {
	return &Robj{typ, encoding, val}
}

func (o *Robj) Val() any {
	return o.val
}

func (o *Robj) SetVal(val any) {
	o.val = val
}

func (o *Robj) SetEncoding(encoding EncodingType) {
	o.encoding = encoding
}

func (o *Robj) Encoding() EncodingType {
	return o.encoding
}

func (o *Robj) SetType(typ RobjType) {
	o.typ = typ
}

func (o *Robj) Type() RobjType {
	return o.typ
}

func (o *Robj) CheckType(typ RobjType) bool {
	return o.typ == typ
}

func (o *Robj) CheckEncoding(encoding EncodingType) bool {
	return o.encoding == encoding
}
