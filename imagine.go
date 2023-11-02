package imagine

import (
	"context"
)

var im IMapStorage = &ImagineMap{}

func NewImagineMap(opt ImagineOption) (IMapStorage, error) {
	memmap := NewMemMap(opt.MemMapOpt)
	diskmap, err := NewDiskMap(opt.DiskMapOpt)
	if err != nil {
		return nil, err
	}

	opt.Valid()

	return &ImagineMap{
		memmap:  memmap,
		diskmap: diskmap,
		opt:     opt.ImageOnFuncOption,
	}, nil
}

// ImagineMap use mmap and
type ImagineMap struct {
	memmap  IMapStorage
	diskmap IMapStorage

	opt ImageOnFuncOption
}

func (im *ImagineMap) Has(ctx context.Context, key string) bool {
	var res bool
	var ok bool

	// cb
	defer func() { im.opt.CBHas(ctx, key, res) }()

	// on event
	if ok, res = im.opt.OnHas(ctx, key); ok {
		return res
	}

	res = im.memmap.Has(ctx, key)
	if res {
		return res
	}

	res = im.diskmap.Has(ctx, key)
	return res
}

func (im *ImagineMap) Get(ctx context.Context, key string) (Valuer, error) {
	var resVal Valuer
	var resErr error
	var ok bool

	// cb
	defer func() {
		im.opt.CBGet(ctx, key, resVal, resErr)
	}()

	// on event
	if ok, resVal, resErr = im.opt.OnGet(ctx, key); ok {
		return resVal, resErr
	}

	resVal, resErr = im.memmap.Get(ctx, key)
	if resErr == ErrValueNotExist {
		resVal, resErr = im.diskmap.Get(ctx, key)

		if resVal != nil { // 更新缓存
			resErr = im.memmap.Set(ctx, key, resVal)
		}

		return resVal, resErr
	}

	return resVal, resErr
}

func (im *ImagineMap) Set(ctx context.Context, key string, val Valuer) error {
	var resErr error
	var ok bool

	// cb
	defer func() { im.opt.CBSet(ctx, key, val, resErr) }()

	// on event
	if ok, resErr = im.opt.OnSet(ctx, key, val); ok {
		return resErr
	}

	resErr = im.memmap.Set(ctx, key, val)
	if resErr != nil {
		return resErr
	}

	// 姑且先这样用吧,后面做统一的 log 更新
	resErr = im.diskmap.Set(ctx, key, val)
	return resErr
}

func (im *ImagineMap) Del(ctx context.Context, key string) {
	var ok bool

	// cb
	defer func() { im.opt.CBDel(ctx, key) }()

	// on event
	if ok = im.opt.OnDel(ctx, key); ok {
		return
	}

	// 姑且先两边都删一下，之后把删除的 option 都记录一下，del 的 merge 也进行同步
	im.memmap.Del(ctx, key)
	im.diskmap.Del(ctx, key)
}

func (im *ImagineMap) Range(ctx context.Context, f RangeFunc) {
	var ok bool
	var rf RangeFunc

	rangedKeys := map[string]struct{}{}

	// cb
	defer func() { im.opt.CBRange(ctx, rangedKeys) }()

	ok, rf = im.opt.OnRange(ctx, f)
	if ok {
		f = rf
	}

	im.memmap.Range(ctx, func(ctx context.Context, key string, val Valuer) bool {
		rangedKeys[key] = struct{}{}

		return f(ctx, key, val)
	})

	im.diskmap.Range(ctx, func(ctx context.Context, key string, val Valuer) bool {
		if _, ok := rangedKeys[key]; ok {
			return true
		}

		return f(ctx, key, val)
	})
}

func (im *ImagineMap) Encode(ctx context.Context) ([]byte, error) {
	var ok bool
	var resByte []byte
	var resErr error

	// cb
	defer func() { im.opt.CBEncode(ctx, resByte, resErr) }()

	// on event
	if ok, resByte, resErr = im.opt.OnEncode(ctx); ok {
		return resByte, resErr
	}

	// TODO
	// 根据情况，给 diskmap + logs
	return resByte, resErr
}

func (im *ImagineMap) Decode(ctx context.Context, b []byte) (IMapStorage, error) {
	var ok bool
	var resStorage IMapStorage
	var resErr error

	// cb
	defer func() { im.opt.CBDecode(ctx, b, resStorage, resErr) }()

	// on event
	if ok, resStorage, resErr = im.opt.OnDecode(ctx, b); ok {
		return resStorage, resErr
	}

	resStorage, resErr = im.memmap.Decode(ctx, b)
	if resErr != nil {
		return nil, resErr
	}

	resStorage = &ImagineMap{
		memmap:  resStorage,
		diskmap: NewNilMap(),
	}

	return resStorage, resErr
}

func (im *ImagineMap) MergeMap(ctx context.Context, ims IMapStorage) error {
	var ok bool
	var resErr error

	// cb
	defer func() { im.opt.CBMergeMap(ctx, ims, resErr) }()

	// on event
	if ok, resErr = im.opt.OnMergeMap(ctx, ims); ok {
		return resErr
	}

	resErr = im.memmap.MergeMap(ctx, ims)
	return resErr
}

func (im *ImagineMap) Close(ctx context.Context) error {
	var ok bool
	var resErr error

	// cb
	defer func() { im.opt.CBClose(ctx, resErr) }()

	// on event
	if ok, resErr = im.opt.OnClose(ctx); ok {
		return resErr
	}

	resErr = im.memmap.Close(ctx)
	if resErr != nil {
		return resErr
	}

	resErr = im.diskmap.Close(ctx)
	return resErr
}

func NewNilMap() IMapStorage {
	return &NilMap{}
}

type NilMap struct{}

func (nm *NilMap) Has(ctx context.Context, key string) bool {
	return false
}

func (nm *NilMap) Get(ctx context.Context, key string) (Valuer, error) {
	return nil, nil
}

func (nm *NilMap) Set(ctx context.Context, key string, val Valuer) error {
	return nil
}

func (nm *NilMap) Del(ctx context.Context, key string) {}

func (nm *NilMap) Range(ctx context.Context, f func(ctx context.Context, key string, val Valuer) bool) {
}

func (nm *NilMap) Encode(ctx context.Context) ([]byte, error) {
	return nil, nil
}

func (nm *NilMap) Decode(ctx context.Context, b []byte) (IMapStorage, error) {
	return nil, nil
}

func (nm *NilMap) MergeMap(ctx context.Context, ims IMapStorage) error {
	return nil
}

func (nm *NilMap) Close(ctx context.Context) error {
	return nil
}
