package util

func CondInt16(expr bool, trueVal, falseVal int16) int16 {
	if expr {
		return trueVal
	}
	return falseVal
}

func CondInt32(expr bool, trueVal, falseVal int32) int32 {
	if expr {
		return trueVal
	}
	return falseVal
}

func CondInt64(expr bool, trueVal, falseVal int64) int64 {
	if expr {
		return trueVal
	}
	return falseVal
}
