package util

func CondInt32(expr bool, trueVal, falseVal int32) int32 {
	if expr {
		return trueVal
	}
	return falseVal
}
