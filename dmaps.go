package imagine

import (
	"context"
	"log"
	"os"
	"path"
	"sync"
	"time"

	"github.com/pkg/errors"
)

var (
	ErrMapNotFound = errors.New("map not found")
)

type DmapsOption struct {
	Dir string
}

func NewDmaps(opt DmapsOption) (*Dmaps, error) {
	metaFile, err := getDmapsMeta(opt.Dir)
	if err != nil {
		return nil, err
	}

	maps := map[string]IMapStorage{}

	// 处理 files
	dataMetaFiles := map[string]DmapsDataFileMeta{}

	for _, dfm := range metaFile.DataFiles {
		dataMetaFiles[dfm.Name] = dfm

		err = dfm.BuildDataFile(opt.Dir)
		if err != nil {
			return nil, errors.Wrap(err, "build datafile fail")
		}

		dataMetaFiles[dfm.Name] = dfm
	}

	for _, mm := range metaFile.Maps {
		_, ok := maps[mm.Name]
		if ok {
			return nil, errors.Wrapf(err, "repeated map namespace : %s", mm.Name)
		}

		var vfunc ValueFunc
		var err error

		vfunc, err = GetValuer(mm.Valuer.Name)
		if err != nil {
			return nil, errors.Wrapf(err, "get map [%s] valuer [%s] fail", mm.Name, mm.Valuer.Name)
		}

		// 这里将来可以做成注册机制，可自定义实现
		switch mm.Engine {
		case "inmemory":
			im := NewMemMap(MemMapOpt{ValueFunc: vfunc})
			maps[mm.Name] = im

		case "disk":
			fm, ok := dataMetaFiles[mm.Data.DataFile]
			if !ok {
				return nil, errors.Wrapf(err, "get datafile fail of %s", mm.Data.DataFile)
			}

			p := path.Join(opt.Dir, mm.Index.FileName)

			idxf, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE, 0644)
			if err != nil {
				return nil, errors.Wrapf(err, "open index file %s of %s", mm.Index.FileName, mm.Name)
			}

			opt := BuildDiskMapOptWithFile(fm.file, fm.bmfile, idxf, fm.PageSize, vfunc)
			dm, err := NewDiskMap(opt)
			if err != nil {
				return nil, errors.Wrapf(err, "new disk map fail of %s", mm.Name)
			}

			maps[mm.Name] = dm

		case "dmap":
			fm, ok := dataMetaFiles[mm.Data.DataFile]
			if !ok {
				return nil, errors.Wrapf(err, "get datafile fail of %s", mm.Data.DataFile)
			}

			p := path.Join(opt.Dir, mm.Index.FileName)

			idxf, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE, 0644)
			if err != nil {
				return nil, errors.Wrapf(err, "open index file %s of %s", mm.Index.FileName, mm.Name)
			}

			dm, err := NewImagineMap(ImagineOption{
				MemMapOpt: MemMapOpt{ValueFunc: vfunc, CacheOpt: CacheOption{
					Enable:     mm.CacheManager.Enable,
					CacheSize:  mm.CacheManager.CacheSize,
					Expiration: time.Duration(mm.CacheManager.ExpirationSec) * time.Second,
				}},

				DiskMapOpt: BuildDiskMapOptWithFile(fm.file, fm.bmfile, idxf, fm.PageSize, vfunc),
			})

			if err != nil {
				return nil, errors.Wrapf(err, "create dmap fail of %s", mm.Name)
			}

			maps[mm.Name] = dm

		default:
			return nil, errors.Errorf("no such map engine of %s", mm.Engine)
		}
	}

	dm := &Dmaps{
		dir:  opt.Dir,
		maps: maps,
	}

	return dm, nil
}

type Dmaps struct {
	dir string

	maps map[string]IMapStorage

	mu sync.RWMutex
}

func (d *Dmaps) MustGetMap(ctx context.Context, ns string) IMapStorage {
	d.mu.RLock()
	defer d.mu.RUnlock()

	m, ok := d.maps[ns]
	if !ok {
		panic(errors.Errorf("must get map %s failed", ns))
	}

	return m
}

func (d *Dmaps) GetMap(ctx context.Context, ns string) (IMapStorage, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	ims, ok := d.maps[ns]
	return ims, ok
}

func (d *Dmaps) Has(ctx context.Context, ns string, key string) bool {
	im, ok := d.GetMap(ctx, ns)
	if !ok {
		return false
	}
	return im.Has(ctx, key)
}

func (d *Dmaps) Get(ctx context.Context, ns string, key string) (Valuer, error) {
	im, ok := d.GetMap(ctx, ns)
	if !ok {
		return nil, ErrMapNotFound
	}
	return im.Get(ctx, key)
}

func (d *Dmaps) Set(ctx context.Context, ns string, key string, val Valuer) error {
	im, ok := d.GetMap(ctx, ns)
	if !ok {
		return ErrMapNotFound
	}
	return im.Set(ctx, key, val)
}
func (d *Dmaps) Del(ctx context.Context, ns string, key string) {
	im, ok := d.GetMap(ctx, ns)
	if !ok {
		return
	}

	im.Del(ctx, key)
}
func (d *Dmaps) Range(ctx context.Context, ns string, f func(ctx context.Context, key string, value Valuer) bool) {
	im, ok := d.GetMap(ctx, ns)
	if !ok {
		return
	}
	im.Range(ctx, f)
}

func (d *Dmaps) Encode(ctx context.Context, ns string) ([]byte, error) {
	im, ok := d.GetMap(ctx, ns)
	if !ok {
		return nil, ErrMapNotFound
	}

	return im.Encode(ctx)
}
func (d *Dmaps) Decode(ctx context.Context, ns string, b []byte) (IMapStorage, error) {
	im, ok := d.GetMap(ctx, ns)
	if !ok {
		return nil, ErrMapNotFound
	}

	return im.Decode(ctx, b)
}

func (d *Dmaps) MergeMap(ctx context.Context, ns string, ims IMapStorage) error {
	im, ok := d.GetMap(ctx, ns)
	if !ok {
		return ErrMapNotFound
	}
	return im.MergeMap(ctx, ims)
}

func (d *Dmaps) CloseNs(ctx context.Context, nss ...string) error {
	var err error

	for _, ns := range nss {
		im, ok := d.GetMap(ctx, ns)
		if !ok {
			return ErrMapNotFound
		}

		// bundle errs
		err = im.Close(ctx)
	}

	return err
}

func (d *Dmaps) Close(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	for _, m := range d.maps {
		err := m.Close(ctx)
		if err != nil {
			log.Printf("close fail : %s", err)
		}
	}

	// bundle errs
	return nil
}
