package dict

type MapDict struct {
	dict map[string]Robj
}

type Robj struct {
	_type uint8
	val   any
}

func NewMap() *MapDict {
	return &MapDict{
		dict: make(map[string]Robj),
	}
}

func (d *MapDict) Add(key string, val Robj) bool {
	_, ok := d.dict[key]
	if ok {
		return false
	}
	d.dict[key] = val
	return true
}

func (d *MapDict) Replace(key string, val Robj) bool {
	d.dict[key] = val
	return true
}

func (d *MapDict) Del(key string) bool {
	_, ok := d.dict[key]
	if ok {
		return false
	}
	delete(d.dict, key)
	return true
}

func (d *MapDict) Get(key string) (Robj, bool) {
	val, ok := d.dict[key]
	return val, ok
}
