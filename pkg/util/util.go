package util

import (
	"math"
)

func Cond[T int16 | int32 | int | int64 | string](expr bool, t, f T) T {
	if expr {
		return t
	}
	return f
}

var digits []byte

func init() {
	digits = make([]byte, 200, 200)
	copy(digits[0:40], []byte("0001020304050607080910111213141516171819"))
	copy(digits[40:80], []byte("2021222324252627282930313233343536373839"))
	copy(digits[80:120], []byte("4041424344454647484950515253545556575859"))
	copy(digits[120:160], []byte("6061626364656667686970717273747576777879"))
	copy(digits[160:200], []byte("8081828384858687888990919293949596979899"))
}

// Int64ToBytes convert the integer of type int64 to byte slice.
// eg: -100 -> "-100"
func Int64ToBytes(n int64) []byte {
	var un uint64
	var negative bool
	if n < 0 {
		if n != math.MinInt64 {
			un = uint64(-n)
		} else {
			un = uint64(math.MaxInt64) + 1
		}
		negative = true
	} else {
		un = uint64(n)
		negative = false
	}

	ln := Digit10(un)
	if negative {
		ln += 1
	}
	dst := make([]byte, ln, ln)
	next := ln
	i := 0
	for un >= 100 {
		// (un % 100) is the last two digits of the integer.
		// and i is the index in digits which corresponding with (un % 100).
		i = int((un % 100)) * 2
		dst[next-1] = digits[i+1]
		dst[next-2] = digits[i]
		un /= 100
		next -= 2
	}

	// 0-9
	if un < 10 {
		dst[next-1] = '0' + byte(un&0xff)
	} else { // 10-99
		i = int(un * 2)
		dst[next-1] = digits[i+1]
		dst[next-2] = digits[i]
	}

	if negative {
		dst[0] = '-'
	}
	return dst
}

func Digit10(n uint64) int64 {
	var ln int64
	if n < 0 {
		ln = 1
		n = -n
	}

	if n < 10 {
		return ln + 1
	}
	if n < 100 {
		return ln + 2
	}
	if n < 1000 {
		return ln + 3
	}
	// < 12
	if n < 1000000000000 {
		// 4-8
		if n < 100000000 {
			// 4-6
			if n < 1000000 {
				// 4 [1000, 9999]
				if n < 10000 {
					return ln + 4
				}
				// 5-6
				return ln + 5 + cond(n >= 100000)
			}
			// 7-8
			return ln + 7 + cond(n >= 10000000)
		}
		if n < 10000000000 {
			return ln + 9 + cond(n >= 1000000000)
		}
		return ln + 11 + cond(n >= 100000000000)
	}
	return ln + 12 + Digit10(n/1000000000000)
}

func cond(c bool) int64 {
	if c {
		return 1
	}
	return 0
}
