package imagine

import (
	"context"
	"sync"

	json "github.com/json-iterator/go"
)

var insmmap IMapStorage = &MemMap{}

type MemMapOpt struct {
	ValueFunc ValueFunc
}

func NewMemMap(opt MemMapOpt) IMapStorage {
	if opt.ValueFunc == nil {
		opt.ValueFunc = DecodeBytesValue
	}

	return &MemMap{
		m:         make(map[string]Value, 0),
		valueFunc: opt.ValueFunc,
	}
}

type MemMap struct {
	mu sync.RWMutex

	m map[string]Value

	// 从 bytes 到 value
	valueFunc ValueFunc
}

func (mm *MemMap) Has(ctx context.Context, key string) bool {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	_, ok := mm.m[key]
	return ok
}

func (mm *MemMap) Get(ctx context.Context, key string) (Value, error) {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	v, ok := mm.m[key]
	if ok {
		return v, nil
	}
	return nil, ErrValueNotExist
}

func (mm *MemMap) Set(ctx context.Context, key string, val Value) error {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	mm.m[key] = val

	return nil
}

func (mm *MemMap) Del(ctx context.Context, key string) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	delete(mm.m, key)
}

func (mm *MemMap) Range(ctx context.Context, f func(ctx context.Context, key string, value Value) bool) {
	// TODO 并发问题
	for k, v := range mm.m {
		if !f(ctx, k, v) {
			return
		}
	}
}

func (mm *MemMap) Encode(ctx context.Context) ([]byte, error) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	tars := make(MapStrBytes, len(mm.m))

	var err error

	mm.Range(ctx, func(ctx context.Context, key string, value Value) bool {
		var b []byte

		b, err = value.Encode()
		if err != nil {
			return false
		}

		tars[key] = ConvertBytesValue(b)
		return true
	})

	if err != nil {
		return nil, err
	}

	// 姑且先用 json encoder
	return json.Marshal(mm.m)
}

func (mm *MemMap) MergeMap(ctx context.Context, ims IMapStorage) error {
	// TODO 处理 回滚等问题
	var err error
	ims.Range(ctx, func(ctx context.Context, key string, value Value) bool {
		err = dm.Set(ctx, key, value)
		if err != nil {
			return false
		}

		return true
	})

	return err
}

func (mm *MemMap) Decode(ctx context.Context, b []byte) (IMapStorage, error) {
	mb := make(MapStrBytes)
	err := json.Unmarshal(b, &mb)
	if err != nil {
		return nil, err
	}

	nmm := &MemMap{
		valueFunc: mm.valueFunc,
		m:         make(map[string]Value),
	}

	for k, v := range mb {
		iv, err := nmm.valueFunc(ctx, *v)
		if err != nil {
			return nil, err
		}

		nmm.m[k] = iv
	}

	return nmm, nil
}

func (mm *MemMap) Close(ctx context.Context) error {
	return nil
}

type BytesValue []byte

func (be *BytesValue) Encode() ([]byte, error) {
	return *be, nil
}

func (be *BytesValue) Decode(b []byte) error {
	*be = b
	return nil
}

func DecodeBytesValue(ctx context.Context, b []byte) (Value, error) {
	bv := BytesValue(b)
	return &bv, nil
}

func ConvertBytesValue(b []byte) *BytesValue {
	bv := BytesValue(b)
	return &bv
}

type MapStrBytes map[string]*BytesValue
