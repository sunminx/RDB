package object

type RobjType int

const (
	UnknownType RobjType = iota
	ObjString
	ObjList
	ObjHash
)

type EncodingType int

const (
	UnknownEncodingType EncodingType = iota
	ObjEncodingInt
	ObjEncodingRaw
	ObjEncodingZiplist
	ObjEncodingQuicklist
)

type Robj struct {
	_type    RobjType
	encoding EncodingType
	val      any
}

func NewRobj(val any) *Robj {
	return &Robj{val: val}
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

func (o *Robj) SetType(_type RobjType) {
	o._type = _type
}

func (o *Robj) Type() RobjType {
	return o._type
}

func (o *Robj) SDSEncodedObject() bool {
	return o.encoding == ObjEncodingRaw
}

func (o *Robj) CheckType(_type RobjType) bool {
	return o._type == _type
}

func (o *Robj) CheckEncoding(encoding EncodingType) bool {
	return o.encoding == encoding
}
