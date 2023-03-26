package imagine

import (
	"context"
	"errors"
	"io"
)

var (
	ErrValueNotExist = errors.New("value not exist")
)

type IMapStorage interface {
	Has(ctx context.Context, key string) bool

	Get(ctx context.Context, key string) (Valuer, error)
	Set(ctx context.Context, key string, val Valuer) error
	Del(ctx context.Context, key string)
	Range(ctx context.Context, f func(ctx context.Context, key string, value Valuer) bool)

	Encode(ctx context.Context) ([]byte, error)
	Decode(ctx context.Context, b []byte) (IMapStorage, error)

	MergeMap(ctx context.Context, ims IMapStorage) error

	Close(ctx context.Context) error
}

type ValuerFactory interface {
	SetValue(i interface{}) ValuerFactory
	SetBytes(b []byte) ValuerFactory
	SetReveiver(i interface{}) ValuerFactory

	Valuer() Valuer
}

type Valuer interface {
	Encoder() Encoder
	Decoder() Decoder

	Value() interface{}
}

type Encoder interface {
	Encode() ([]byte, error)
}

type Decoder interface {
	Decode() error
	DecodeTo(ptr interface{}) error
}

// type MapMerger interface {
// 	Merge(f func(ctx context.Context, k string, val Valuer) error)
// }

type IReaderWriterAt interface {
	io.Reader
	io.ReaderAt
	io.Writer
	io.WriterAt
	io.Closer
}
