package msghub

import "time"

var _start = time.Now()

func Monotonic() time.Duration { // 单调递增时间
	return time.Now().Sub(_start)
}
