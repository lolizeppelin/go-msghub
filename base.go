package msghub

import (
	"context"
	"time"
)

type message struct {
	ctx      context.Context
	resource string
	event    string
	trigger  string
	payload  any

	index    int
	priority time.Duration
}
