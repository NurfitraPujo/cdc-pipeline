package retry

import (
	"time"

	"github.com/avast/retry-go/v4"
)

var DefaultOptions = []retry.Option{
	retry.LastErrorOnly(true),
	retry.Delay(time.Second),
	retry.DelayType(retry.FixedDelay),
}

type Config[T any] struct {
	If      func(err error) bool
	Options []retry.Option
}

func (rc Config[T]) Do(f retry.RetryableFuncWithData[T]) (T, error) {
	// vendored-patch: T2-6 - Register the If callback as RetryIf so custom filters are honored
	opts := append(rc.Options, retry.RetryIf(rc.If))
	return retry.DoWithData(f, opts...)
}

func OnErrorConfig[T any](attemptCount uint, check func(error) bool) Config[T] {
	cfg := Config[T]{
		If:      check,
		Options: []retry.Option{retry.Attempts(attemptCount)},
	}
	cfg.Options = append(cfg.Options, DefaultOptions...)
	return cfg
}
