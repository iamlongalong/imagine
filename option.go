package imagine

import (
	"context"
	"os"
	"path"

	json "github.com/json-iterator/go"
	"github.com/pkg/errors"
)

// =========== imagine

// 姑且先用 func 吧，之后可以结合 ValuerFactory 看看
type ValueFunc = func(ctx context.Context, b []byte) (Valuer, error)

type RangeFunc = func(ctx context.Context, key string, val Valuer) bool

type OnHas = func(ctx context.Context, key string) (ok bool, has bool)
type OnGet = func(ctx context.Context, key string) (ok bool, val Valuer, err error)
type OnSet = func(ctx context.Context, key string, val Valuer) (ok bool, err error)
type OnDel = func(ctx context.Context, key string) (ok bool)
type OnRange = func(ctx context.Context, orifunc RangeFunc) (ok bool, f RangeFunc)
type OnEncode = func(ctx context.Context) (ok bool, b []byte, err error)
type OnDecode = func(ctx context.Context, b []byte) (ok bool, ims IMapStorage, err error)
type OnMergeMap = func(ctx context.Context, ims IMapStorage) (ok bool, err error)
type OnClose = func(ctx context.Context) (ok bool, err error)

type CBHas = func(ctx context.Context, key string, has bool)
type CBGet = func(ctx context.Context, key string, val Valuer, err error)
type CBSet = func(ctx context.Context, key string, val Valuer, err error)
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
		iof.OnGet = func(ctx context.Context, key string) (ok bool, val Valuer, err error) { return false, nil, nil }
	}
	if iof.OnSet == nil {
		iof.OnSet = func(ctx context.Context, key string, val Valuer) (ok bool, err error) { return false, nil }
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
		iof.CBGet = func(ctx context.Context, key string, val Valuer, err error) {}
	}
	if iof.CBSet == nil {
		iof.CBSet = func(ctx context.Context, key string, val Valuer, err error) {}
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

// ========================== dmaps

type DmapsMeta struct {
	Maps      []DmapsMapMeta
	DataFiles []DmapsDataFileMeta
}

func (dm *DmapsMeta) GetMapMeta(name string) (DmapsMapMeta, bool) {
	for _, m := range dm.Maps {
		if m.Name == name {
			return m, true
		}
	}

	return DmapsMapMeta{}, false
}

func (dm *DmapsMeta) GetDataFileMeta(name string) (DmapsDataFileMeta, bool) {
	for _, m := range dm.DataFiles {
		if m.Name == name {
			return m, true
		}
	}

	return DmapsDataFileMeta{}, false
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
	Valuer struct {
		Name string
	}
	CacheManager struct {
		Enable        bool
		CacheSize     int
		ExpirationSec int
	}
	Options json.RawMessage
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

func getDmapsMeta(dir string) (*DmapsMeta, error) {
	metaFile := &DmapsMeta{
		Maps:      make([]DmapsMapMeta, 0),
		DataFiles: make([]DmapsDataFileMeta, 0),
	}

	metafilePath := path.Join(dir, "meta.json")
	mb, err := os.ReadFile(metafilePath)
	if err != nil {
		return nil, errors.Wrap(err, "open meta file fail")
	}

	err = json.Unmarshal(mb, metaFile)
	if err != nil {
		return nil, errors.Wrap(err, "unmashal metafile fail")
	}

	return metaFile, nil
}
