package imagine

import (
	"context"
	"errors"
)

var (
	ErrValueNotExist = errors.New("value not exist")
)

type IMapStorage interface {
	Has(ctx context.Context, key string) bool

	Get(ctx context.Context, key string) (Value, error)
	Set(ctx context.Context, key string, val Value) error
	Del(ctx context.Context, key string)
	Range(ctx context.Context, f func(ctx context.Context, key string, value Value) bool)

	Encode(ctx context.Context) ([]byte, error)
	Decode(ctx context.Context, b []byte) (IMapStorage, error)
}

type Value interface {
	Encode() ([]byte, error)
	Decode(b []byte) error
}

type MapMerger interface {
	Merge(f func(ctx context.Context, k string, val Value) error)
}

func x() {
	// m := sync.Map{}
}
