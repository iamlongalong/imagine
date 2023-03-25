package imagine

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"path"
	"sync"

	"github.com/pkg/errors"
)

var (
	ErrMapNotFound = errors.New("map not found")
)

type DmapsOption struct {
	Dir string
}

type DmapsMeta struct {
	Maps      []DmapsMapMeta
	DataFiles []DmapsDataFileMeta
}

type DmapsMapMeta struct {
	Name   string
	Engine string
	Index  struct {
		FileName  string
		IndexMeta interface{}
	}
	Data struct {
		DataFile string
	}
}

type DmapsDataFileMeta struct {
	Name           string
	FileName       string
	PageSize       int
	PagesNum       int
	PageBitMapFile string

	file   *os.File
	bmfile *os.File // 后端可以改成 file 中的一个段
}

func (dfm *DmapsDataFileMeta) BuildDataFile(basePath string) error {
	f, err := os.OpenFile(path.Join(basePath, dfm.FileName), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}

	bf, err := os.OpenFile(path.Join(basePath, dfm.PageBitMapFile), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}

	dfm.bmfile = bf
	dfm.file = f

	return nil
}

func NewDmaps(opt DmapsOption) (*Dmaps, error) {
	metafilePath := path.Join(opt.Dir, "meta.json")
	mb, err := os.ReadFile(metafilePath)
	if err != nil {
		return nil, errors.Wrap(err, "open meta file fail")
	}

	metaFile := &DmapsMeta{
		Maps:      make([]DmapsMapMeta, 0),
		DataFiles: make([]DmapsDataFileMeta, 0),
	}

	err = json.Unmarshal(mb, metaFile)
	if err != nil {
		return nil, errors.Wrap(err, "unmashal metafile fail")
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

		// 这里将来可以做成注册机制，可自定义实现
		switch mm.Engine {
		case "inmemory":
			im := NewMemMap(MemMapOpt{ValueFunc: DecodeBytesValue})
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

			opt := BuildDiskMapOptWithFile(fm.file, fm.bmfile, idxf, fm.PageSize, DecodeBytesValue)
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
				MemMapOpt:  MemMapOpt{ValueFunc: DecodeBytesValue},
				DiskMapOpt: BuildDiskMapOptWithFile(fm.file, fm.bmfile, idxf, fm.PageSize, DecodeBytesValue),
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

	return d.maps[ns]
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

func (d *Dmaps) Get(ctx context.Context, ns string, key string) (Value, error) {
	im, ok := d.GetMap(ctx, ns)
	if !ok {
		return nil, ErrMapNotFound
	}
	return im.Get(ctx, key)
}

func (d *Dmaps) Set(ctx context.Context, ns string, key string, val Value) error {
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
func (d *Dmaps) Range(ctx context.Context, ns string, f func(ctx context.Context, key string, value Value) bool) {
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