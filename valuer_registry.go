package imagine

import (
	"context"
	"sync"

	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
)

var dfregistry IValuerRegistry = &ValuerRegistry{vm: make(map[string]func(ctx context.Context, b []byte) (Value, error))}

var BytesValuerName = "bytes_valuer"
var JsonValuerName = "json_valuer"

func init() {
	RegistryValuer(BytesValuerName, DecodeBytesValue)
	RegistryValuer(JsonValuerName, DecodeJsonValue)
}

type IValuerRegistry interface {
	Registry(name string, valfunc ValueFunc) error
	GetValuer(name string) (ValueFunc, error)
}

type ValuerRegistry struct {
	vm map[string]ValueFunc

	mu sync.Mutex
}

var (
	ErrValuerAlreadyExists = errors.New("valuer already exists")
	ErrValuerNotExists     = errors.New("valuer not exists")
)

func RegistryValuer(name string, valfunc ValueFunc) error {
	return dfregistry.Registry(name, valfunc)
}

func GetValuer(name string) (valfunc ValueFunc, err error) {
	return dfregistry.GetValuer(name)
}

func (vr *ValuerRegistry) Registry(name string, valfunc ValueFunc) error {
	vr.mu.Lock()
	defer vr.mu.Unlock()

	_, ok := vr.vm[name]
	if ok {
		return errors.Wrap(ErrValuerAlreadyExists, name)
	}

	vr.vm[name] = valfunc
	return nil
}

func (vr *ValuerRegistry) GetValuer(name string) (ValueFunc, error) {
	vr.mu.Lock()
	defer vr.mu.Unlock()

	v, ok := vr.vm[name]
	if !ok {
		return nil, errors.Wrap(ErrValuerNotExists, name)
	}

	return v, nil
}

func DecodeBytesValue(ctx context.Context, b []byte) (Value, error) {
	return ConvertBytesValue(b), nil
}

func ConvertBytesValue(b []byte) *BytesValue {
	bv := BytesValue(b)
	return &bv
}

// BytesValue bytes valuer
type BytesValue []byte

func (be *BytesValue) Encode() ([]byte, error) {
	return *be, nil
}

func (be *BytesValue) Decode(b []byte) error {
	*be = b
	return nil
}

// json valuer
func DecodeJsonValue(ctx context.Context, b []byte) (Value, error) {
	jv := &JsonValue{b: b}

	if ok := jsoniter.Valid(b); !ok {
		return nil, errors.Errorf("invalid json format")
	}

	return jv, nil
}

func ConvertJsonValue(v interface{}) *JsonValue {
	jv := &JsonValue{
		v: v,
	}

	return jv
}

type JsonValue struct {
	v interface{}
	b []byte
}

func (jv *JsonValue) Encode() ([]byte, error) {
	return jsoniter.Marshal(jv.v)
}

// Decode must be pointer
func (jv *JsonValue) Decode(v interface{}) error {
	return jsoniter.Unmarshal(jv.b, v)
}

type Decoder interface {
	Decode(ptr interface{}) error
}
