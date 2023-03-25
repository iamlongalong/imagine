package imagine

import (
	"context"
)

type ValueFunc = func(ctx context.Context, b []byte) (Value, error)
type RangeFunc = func(ctx context.Context, key string, value Value) bool

var im IMapStorage = &ImagineMap{}

type OnHas = func(ctx context.Context, key string) (ok bool, has bool)
type OnGet = func(ctx context.Context, key string) (ok bool, val Value, err error)
type OnSet = func(ctx context.Context, key string, val Value) (ok bool, err error)
type OnDel = func(ctx context.Context, key string) (ok bool)
type OnRange = func(ctx context.Context, orifunc RangeFunc) (ok bool, f RangeFunc)
type OnEncode = func(ctx context.Context) (ok bool, b []byte, err error)
type OnDecode = func(ctx context.Context, b []byte) (ok bool, ims IMapStorage, err error)
type OnMergeMap = func(ctx context.Context, ims IMapStorage) (ok bool, err error)
type OnClose = func(ctx context.Context) (ok bool, err error)

type CBHas = func(ctx context.Context, key string, has bool)
type CBGet = func(ctx context.Context, key string, val Value, err error)
type CBSet = func(ctx context.Context, key string, val Value, err error)
type CBDel = func(ctx context.Context, key string)
type CBRange = func(ctx context.Context, rangedKeys map[string]struct{})
type CBEncode = func(ctx context.Context, b []byte, err error)
type CBDecode = func(ctx context.Context, b []byte, ims IMapStorage, err error)
type CBMergeMap = func(ctx context.Context, ims IMapStorage, err error)
type CBClose = func(ctx context.Context, err error)

type ImagineOption struct {
	MemMapOpt
	DiskMapOpt

	ImageOnFuncOption
}

type ImageOnFuncOption struct {
	OnHas
	OnGet
	OnSet
	OnDel
	OnRange
	OnEncode
	OnDecode
	OnMergeMap
	OnClose

	CBHas
	CBGet
	CBSet
	CBDel
	CBRange
	CBEncode
	CBDecode
	CBMergeMap
	CBClose
}

func (iof *ImageOnFuncOption) Valid() {
	if iof.OnHas == nil {
		iof.OnHas = func(ctx context.Context, key string) (ok bool, has bool) { return false, false }
	}
	if iof.OnGet == nil {
		iof.OnGet = func(ctx context.Context, key string) (ok bool, val Value, err error) { return false, nil, nil }
	}
	if iof.OnSet == nil {
		iof.OnSet = func(ctx context.Context, key string, val Value) (ok bool, err error) { return false, nil }
	}
	if iof.OnDel == nil {
		iof.OnDel = func(ctx context.Context, key string) (ok bool) { return false }
	}
	if iof.OnRange == nil {
		iof.OnRange = func(ctx context.Context, orifunc RangeFunc) (ok bool, f RangeFunc) { return false, nil }
	}
	if iof.OnEncode == nil {
		iof.OnEncode = func(ctx context.Context) (ok bool, b []byte, err error) { return false, nil, nil }
	}
	if iof.OnDecode == nil {
		iof.OnDecode = func(ctx context.Context, b []byte) (ok bool, ims IMapStorage, err error) { return false, nil, nil }
	}
	if iof.OnMergeMap == nil {
		iof.OnMergeMap = func(ctx context.Context, ims IMapStorage) (ok bool, err error) { return false, nil }
	}
	if iof.OnClose == nil {
		iof.OnClose = func(ctx context.Context) (ok bool, err error) { return false, nil }
	}

	if iof.CBHas == nil {
		iof.CBHas = func(ctx context.Context, key string, has bool) {}
	}
	if iof.CBGet == nil {
		iof.CBGet = func(ctx context.Context, key string, val Value, err error) {}
	}
	if iof.CBSet == nil {
		iof.CBSet = func(ctx context.Context, key string, val Value, err error) {}
	}
	if iof.CBDel == nil {
		iof.CBDel = func(ctx context.Context, key string) {}
	}
	if iof.CBRange == nil {
		iof.CBRange = func(ctx context.Context, rangedKeys map[string]struct{}) {}
	}
	if iof.CBEncode == nil {
		iof.CBEncode = func(ctx context.Context, b []byte, err error) {}
	}
	if iof.CBDecode == nil {
		iof.CBDecode = func(ctx context.Context, b []byte, ims IMapStorage, err error) {}
	}
	if iof.CBMergeMap == nil {
		iof.CBMergeMap = func(ctx context.Context, ims IMapStorage, err error) {}
	}
	if iof.CBClose == nil {
		iof.CBClose = func(ctx context.Context, err error) {}
	}

}

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

func (im *ImagineMap) Get(ctx context.Context, key string) (Value, error) {
	var resVal Value
	var resErr error
	var ok bool

	// cb
	defer func() { im.opt.CBGet(ctx, key, resVal, resErr) }()

	// on event
	if ok, resVal, resErr = im.opt.OnGet(ctx, key); ok {
		return resVal, resErr
	}

	resVal, resErr = im.memmap.Get(ctx, key)
	if resErr == ErrValueNotExist {
		resVal, resErr = im.diskmap.Get(ctx, key)
		return resVal, resErr
	}

	return resVal, resErr
}

func (im *ImagineMap) Set(ctx context.Context, key string, val Value) error {
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

	alreadyKeys := map[string]struct{}{}

	// cb
	defer func() { im.opt.CBRange(ctx, alreadyKeys) }()

	ok, rf = im.opt.OnRange(ctx, f)
	if ok {
		f = rf
	}

	im.memmap.Range(ctx, func(ctx context.Context, key string, value Value) bool {
		alreadyKeys[key] = struct{}{}

		return f(ctx, key, value)
	})

	im.diskmap.Range(ctx, func(ctx context.Context, key string, value Value) bool {
		if _, ok := alreadyKeys[key]; ok {
			return true
		}

		return f(ctx, key, value)
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
	// TODO
	// 把传进来的 bytes 变为当前的 imap

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

func (nm *NilMap) Get(ctx context.Context, key string) (Value, error) {
	return nil, nil
}

func (nm *NilMap) Set(ctx context.Context, key string, val Value) error {
	return nil
}

func (nm *NilMap) Del(ctx context.Context, key string) {}

func (nm *NilMap) Range(ctx context.Context, f func(ctx context.Context, key string, value Value) bool) {
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
