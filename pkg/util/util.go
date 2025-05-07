package util

func Cond[T int16 | int32 | int64 | string](expr bool, t, f T) T {
	if expr {
		return t
	}
	return f
}
