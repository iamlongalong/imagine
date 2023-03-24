package imagine

import (
	"context"
)

type ValueFunc = func(ctx context.Context, b []byte) (Value, error)

var im IMapStorage = &ImagineMap{}

type ImagineOption struct {
	MemMapOpt
	DiskMapOpt
}

func NewImagineMap(opt ImagineOption) (IMapStorage, error) {
	memmap := NewMemMap(opt.MemMapOpt)
	diskmap, err := NewDiskMap(opt.DiskMapOpt)
	if err != nil {
		return nil, err
	}

	return &ImagineMap{
		memmap:  memmap,
		diskmap: diskmap,
	}, nil
}

// ImagineMap use mmap and
type ImagineMap struct {
	memmap  IMapStorage
	diskmap IMapStorage
}

func (im *ImagineMap) Has(ctx context.Context, key string) bool {
	ok := im.memmap.Has(ctx, key)
	if ok {
		return ok
	}

	return im.diskmap.Has(ctx, key)
}

func (im *ImagineMap) Get(ctx context.Context, key string) (Value, error) {
	v, err := im.memmap.Get(ctx, key)
	if err == ErrValueNotExist {
		v, err = im.diskmap.Get(ctx, key)
		return v, err
	}

	return v, err
}

func (im *ImagineMap) Set(ctx context.Context, key string, val Value) error {
	return im.memmap.Set(ctx, key, val)
}

func (im *ImagineMap) Del(ctx context.Context, key string) {
	// 姑且先两边都删一下，之后把删除的 option 都记录一下，del 的 merge 也进行同步
	im.memmap.Del(ctx, key)
	im.diskmap.Del(ctx, key)
}

func (im *ImagineMap) Range(ctx context.Context, f func(ctx context.Context, key string, value Value) bool) {
	alreadyKeys := map[string]struct{}{}
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
	// TODO
	// 根据情况，给 diskmap + logs

	return nil, nil
}
func (im *ImagineMap) Decode(ctx context.Context, b []byte) (IMapStorage, error) {
	// TODO
	// 把传进来的 bytes 变为当前的 imap
	nmm, err := im.memmap.Decode(ctx, b)
	if err != nil {
		return nil, err
	}

	nim := &ImagineMap{
		memmap:  nmm,
		diskmap: NewNilMap(),
	}

	return nim, nil
}

func (im *ImagineMap) MergeMap(ctx context.Context, ims IMapStorage) error {
	return im.memmap.MergeMap(ctx, ims)
}

func (im *ImagineMap) Close(ctx context.Context) error {
	err := im.memmap.Close(ctx)
	if err != nil {
		return err
	}

	return im.diskmap.Close(ctx)
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
	return
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
