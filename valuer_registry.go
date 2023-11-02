package imagine

import (
	"bytes"
	"context"
	"encoding/gob"
	"sync"

	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
)

var dfregistry IValuerRegistry = &ValuerRegistry{vm: make(map[string]func(ctx context.Context, b []byte) (Valuer, error))}

var BytesValuerName = "bytes_valuer"
var JsonValuerName = "json_valuer"
var GobValuerName = "gob_valuer"

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
	ErrValuerDuplicatedName = errors.New("valuer duplicate name")
	ErrValuerNotExists      = errors.New("valuer not exists")
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
		return errors.Wrap(ErrValuerDuplicatedName, name)
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

func DecodeBytesValue(ctx context.Context, b []byte) (Valuer, error) {
	return &BytesValue{v: b, b: b}, nil
}

func ConvertBytesValue(b []byte) *BytesValue {
	return &BytesValue{v: b, b: b}
}

// BytesValue bytes valuer
type BytesValue struct {
	v interface{}
	b []byte
}

func (bv *BytesValue) Encoder() Encoder {
	return bv
}

func (bv *BytesValue) Decoder() Decoder {
	return bv
}

func (bv *BytesValue) Encode() ([]byte, error) {
	if bv.v != nil {
		tb, ok := bv.v.([]byte)
		if !ok {
			return nil, errors.Errorf("BytesValue encode fail with wrong type: need []byte, got %t", bv.v)
		}

		return tb, nil
	}

	return bv.b, nil
}

func (bv *BytesValue) SetBytes(b []byte) {
	bv.b = b
}

func (bv *BytesValue) SetValue(i interface{}) Encoder {
	bv.v = i
	return bv
}

func (bv *BytesValue) Value() interface{} {
	return bv.v
}

func (bv *BytesValue) Decode() error {
	if bv.v == nil {
		return errors.Errorf("BytesValue decode fail with nil reciver")
	}

	if bv.b == nil {
		return errors.Errorf("BytesValue decode fail with nil value")
	}

	tb, ok := bv.v.([]byte)
	if !ok {
		return errors.Errorf("BytesValue dncode fail with wrong type: need []byte, got %t", bv.v)
	}

	if &tb == &bv.b { // 预计是比较 slice 本身的地址
		return nil
	}

	copy(tb, bv.b)

	return nil
}

func (bv *BytesValue) DecodeTo(i interface{}) error {
	tb, ok := i.([]byte)
	if !ok {
		return errors.Errorf("DytesValue decode fail with wrong type: need []byte, got %t", i)
	}

	if bv.b == nil {
		return errors.Errorf("BytesValue decode fail with nil value")
	}

	copy(tb, bv.b)

	return nil
}

// json valuer
func DecodeJsonValue(ctx context.Context, b []byte) (Valuer, error) {
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

func (jv *JsonValue) Encoder() Encoder {
	return jv
}

func (jv *JsonValue) Decoder() Decoder {
	return jv
}

func (jv *JsonValue) SetBytes(b []byte) *JsonValue {
	jv.b = b
	return jv
}

func (jv *JsonValue) SetValue(i interface{}) *JsonValue {
	jv.v = i
	return jv
}

func (jv *JsonValue) Value() interface{} {
	return jv.v
}

func (jv *JsonValue) Encode() ([]byte, error) {
	if jv.v != nil {
		b, err := jsoniter.Marshal(jv.v)
		if err != nil {
			return nil, err
		}

		jv.b = b
		return jv.b, nil
	}

	if jv.b != nil {
		return jv.b, nil
	}

	return nil, errors.New("Encode json fail with nil value")
}

func (jv *JsonValue) Decode() error {
	if jv.v == nil {
		return errors.New("Decode json fail with nil reciver")
	}

	if jv.b == nil {
		return errors.New("Decode json fail with nil value")
	}

	return jsoniter.Unmarshal(jv.b, jv.v)
}

// Decode must be pointer
func (jv *JsonValue) DecodeTo(v interface{}) error {
	if jv.b == nil {
		return errors.Errorf("Decode json fail with nil value")
	}

	return jsoniter.Unmarshal(jv.b, v)
}

// json valuer
func DecodeGobValue(ctx context.Context, b []byte) (Valuer, error) {
	v := &GobValue{
		b: b,
	}

	return v, nil
}

func ConvertGobValue(v interface{}) *GobValue {
	jv := &GobValue{
		v: v,
	}

	return jv
}

type GobValue struct {
	v interface{}
	b []byte
}

func (gv *GobValue) Encoder() Encoder {
	return gv
}

func (gv *GobValue) Decoder() Decoder {
	return gv
}

func (gv *GobValue) SetBytes(b []byte) Valuer {
	gv.b = b
	return gv
}

func (gv *GobValue) Value() interface{} {
	return gv.v
}

func (gv *GobValue) SetValue(i interface{}) Valuer {
	gv.v = i
	return gv
}

func (gv *GobValue) Encode() ([]byte, error) {
	if gv.v != nil { // 优先重新 decode

		buf := bytes.NewBuffer(nil)

		enc := gob.NewEncoder(buf)
		if err := enc.Encode(gv.v); err != nil {
			return nil, err
		}

		gv.b = buf.Bytes()

		return buf.Bytes(), nil
	}

	if gv.b != nil { // 其次返回以前的 bytes
		return gv.b, nil
	}

	return nil, errors.New("Encode gob fail with nil data")
}

// Decode must be pointer
func (gv *GobValue) Decode() error {
	if gv.v == nil {
		return errors.New("Decode gob fail with nil receiver")
	}

	if gv.b == nil {
		return errors.New("Decode gob fail with nil data")
	}

	br := bytes.NewReader(gv.b)
	d := gob.NewDecoder(br)

	return d.Decode(gv.v)
}

func (gv *GobValue) DecodeTo(v interface{}) error {
	if gv.b == nil {
		return errors.New("Decode gob fail with nil data")
	}

	br := bytes.NewReader(gv.b)
	d := gob.NewDecoder(br)

	return d.Decode(v)
}
