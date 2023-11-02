package imagine

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/bluele/gcache"
	json "github.com/json-iterator/go"
)

var mmap IMapStorage = &MemMap{}

type MemMapOpt struct {
	ValueFunc ValueFunc

	CacheOpt CacheOption
}

type CacheOption struct {
	Enable     bool
	CacheSize  int
	Expiration time.Duration
}

func (co *CacheOption) Valid() error {
	return nil
}

func NewMemMap(opt MemMapOpt) IMapStorage {
	if opt.ValueFunc == nil {
		opt.ValueFunc = DecodeBytesValue
	}

	im := &MemMap{
		m:         make(map[string]Valuer, 0),
		valueFunc: opt.ValueFunc,
	}

	err := opt.CacheOpt.Valid()
	if err != nil {
		log.Print(err)
	}

	if !opt.CacheOpt.Enable {
		im.cacheManager = gcache.New(0).Build()

	} else {
		im.cacheManager = gcache.New(opt.CacheOpt.CacheSize).LRU().
			// AddedFunc(func(i1, i2 interface{}) {}).
			EvictedFunc(func(i1, i2 interface{}) {
				log.Printf("key %s is evicted", i1)

				im.Del(context.Background(), i1.(string))
			}).
			Expiration(opt.CacheOpt.Expiration).
			// PurgeVisitorFunc(func(i1, i2 interface{}) {}).
			Build()

	}

	return im
}

type MemMap struct {
	mu sync.RWMutex

	m map[string]Valuer

	// 从 bytes 到 value
	valueFunc ValueFunc

	// TODO 先姑且用自己写的 map 吧，回头可以直接用 gcache 替换掉
	cacheManager gcache.Cache
}

func (mm *MemMap) Has(ctx context.Context, key string) bool {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	// update cache
	mm.cacheManager.Set(key, struct{}{})

	_, ok := mm.m[key]
	return ok
}

func (mm *MemMap) Get(ctx context.Context, key string) (Valuer, error) {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	// update cache
	mm.cacheManager.Set(key, struct{}{})

	v, ok := mm.m[key]
	if ok {
		return v, nil
	}
	return nil, ErrValueNotExist
}

func (mm *MemMap) Set(ctx context.Context, key string, val Valuer) error {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	// update cache
	mm.cacheManager.Set(key, struct{}{})

	mm.m[key] = val

	return nil
}

func (mm *MemMap) Del(ctx context.Context, key string) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	// update cache
	mm.cacheManager.Remove(key)

	delete(mm.m, key)
}

func (mm *MemMap) Range(ctx context.Context, f func(ctx context.Context, key string, value Valuer) bool) {
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

	tars := make(mapStrBytes, len(mm.m))

	var err error

	mm.Range(ctx, func(ctx context.Context, key string, value Valuer) bool {
		var b []byte

		b, err = value.Encoder().Encode()
		if err != nil {
			return false
		}

		tars[key] = b
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
	ims.Range(ctx, func(ctx context.Context, key string, value Valuer) bool {
		err = dm.Set(ctx, key, value)
		if err != nil {
			log.Printf("set map [%s] fail : %s", key, err)
			return false
		}

		return true
	})

	return err
}

func (mm *MemMap) Decode(ctx context.Context, b []byte) (IMapStorage, error) {
	mb := make(mapStrBytes)
	err := json.Unmarshal(b, &mb)
	if err != nil {
		return nil, err
	}

	nmm := &MemMap{
		valueFunc: mm.valueFunc,
		m:         make(map[string]Valuer),
	}

	for k, v := range mb {
		iv, err := nmm.valueFunc(ctx, []byte(v))
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

type mapStrBytes map[string]json.RawMessage
