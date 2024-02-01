package msghub

const (
	MaxExecutorsNum  = 8192
	MaxExecutorCache = 100000

	MaxQueueNum   = 256
	MaxQueueCache = 8192
	MaxQueueSize  = 8192
)

type Option func(*Options)

type Options struct {
	// 实时执行协程数量
	executors int
	// 延迟队列数量(hash延迟到n个延迟队列)
	queue int

	// 执行队列管道长度
	executorCache int
	// 延迟队列管道长度
	queueCache int

	// 延迟队列长度
	queueSize int
}

func WithExecutors(num int32) Option {
	return func(o *Options) {
		if num < 0 {
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
		if num < 0 {
			num = 128
		}
		if num >= MaxExecutorCache {
			num = MaxExecutorCache
		}
		o.executorCache = int(num)
	}
}

func WithDelayQueue(num int32) Option {
	return func(o *Options) {
		if num < 0 {
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
		if num < 0 {
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
		if num < 0 {
			num = 128
		}
		if num >= MaxQueueSize {
			num = MaxQueueSize
		}
		o.queueSize = int(num)
	}
}
