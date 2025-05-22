package msghub

const (
	MaxExecutorsNum = 2 * 8192
	MaxMsgCache     = 100000

	MaxQueueNum   = 256
	MaxQueueCache = 8192
	MaxQueueSize  = 8 * 8192

	MaxDeadLetter = 100000
)

type Option func(*Options)

type Options struct {
	// 实时执行协程数量
	executors int
	// 延迟队列数量(hash延迟到n个延迟队列)
	queue int

	// 执行队列管道长度
	msgCache int
	// 延迟队列管道长度
	queueCache int

	// 延迟队列最大长度,超过长度会立刻弹出现有队列中的10个元素

	queueSize int

	deadLetter int32

	log LoginHandler
}

func WithLog(log LoginHandler) Option {
	return func(o *Options) {
		o.log = log
	}
}

func WithExecutors(num int32) Option {
	return func(o *Options) {
		if num <= 0 {
			num = 2
		}
		if num >= MaxExecutorsNum {
			num = MaxExecutorsNum
		}
		o.executors = int(num)
	}
}

func WithExecutorCache(num int32) Option {
	return func(o *Options) {
		if num <= 0 {
			num = 128
		}
		if num >= MaxMsgCache {
			num = MaxMsgCache
		}
		o.msgCache = int(num)
	}
}

func WithMsgCache(num int32) Option {
	return func(o *Options) {
		if num <= 0 {
			num = 128
		}
		if num >= MaxMsgCache {
			num = MaxMsgCache
		}
		o.msgCache = int(num)
	}
}

func WithDelayQueue(num int32) Option {
	return func(o *Options) {
		if num <= 0 {
			num = 1
		}
		if num >= MaxQueueNum {
			num = MaxQueueNum
		}
		o.queue = int(num)
	}
}

func WithDelayQueueCache(num int32) Option {
	return func(o *Options) {
		if num <= 0 {
			num = 128
		}
		if num >= MaxQueueCache {
			num = MaxQueueCache
		}
		o.queueCache = int(num)
	}
}

func WithDelayQueueSize(num int32) Option {
	return func(o *Options) {
		if num <= 0 {
			num = 128
		}
		if num >= MaxQueueSize {
			num = MaxQueueSize
		}
		o.queueSize = int(num)
	}
}

func WithDeadLetter(num int32) Option {
	return func(o *Options) {
		if num <= 0 {
			return
		}
		if num >= MaxDeadLetter {
			num = MaxDeadLetter
		}
		o.deadLetter = num
	}
}
