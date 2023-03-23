package imagine

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sync"

	"github.com/pkg/errors"
)

var (
	ErrSpaceNotEnough = errors.New("space not enough")
	ErrWrongPosiLen   = errors.New("posi []byte len must be 16")

	ErrUnexpectedEnd  = errors.New("read unexpected end")
	ErrFileBlockSize  = errors.New("invalid file block size")
	ErrInvalidPageNum = errors.New("invalid page num")

	dm IMapStorage = &DiskMap{}
)

type DiskMapOpt struct {
	ValueFunc    func(ctx context.Context, b []byte) (Value, error)
	DataFile     *os.File
	IndexManager io.ReadWriter
}

func NewDiskMap(opt DiskMapOpt) (IMapStorage, error) {
	if opt.ValueFunc == nil {
		opt.ValueFunc = DecodeBytesValue
	}

	b, err := ioutil.ReadAll(opt.IndexManager)
	if err != nil {
		return nil, err
	}

	idxManager := NewKeyIndex()
	err = idxManager.Decode(b)
	if err != nil {
		return nil, err
	}

	pageManager, err := NewPageManager(PageManagerOpt{
		PageSize: 512,
		File:     opt.DataFile,
	})
	if err != nil {
		return nil, err
	}

	return &DiskMap{
		valueFunc:    opt.ValueFunc,
		indexManager: idxManager,
		pageManager:  pageManager,
	}, nil
}

type DiskMap struct {
	indexManager *KeyIndex

	pageManager *PageManager

	valueFunc func(ctx context.Context, b []byte) (Value, error)
}

func (dm *DiskMap) Has(ctx context.Context, key string) bool {
	return dm.indexManager.Has(ctx, key)
}

func (dm *DiskMap) Get(ctx context.Context, key string) (Value, error) {
	var baseErr = errors.New("get diskmap fail")

	block, err := dm.getBlock(ctx, key)
	if err != nil {
		return nil, errors.Wrap(err, baseErr.Error())
	}

	tb := make([]byte, block.GetFilledLength())
	n, err := block.Read(tb)
	if err != nil {
		return nil, err
	}

	if n < block.GetFilledLength() {
		return nil, ErrUnexpectedEnd
	}

	v, err := dm.valueFunc(ctx, tb)
	return v, err
}

func (dm *DiskMap) Set(ctx context.Context, key string, val Value) error {
	var baseErr = errors.New("set diskmap fail")

	b, err := val.Encode()
	if err != nil {
		return errors.Wrap(err, baseErr.Error())
	}

	var bl *Block
	var obl *Block

	var useOldBlock bool

	// 1. 看当前 key 的 block 大小是否够，够则直接 write
	obl, err = dm.getBlock(ctx, key)
	if err != nil && !errors.Is(err, ErrValueNotExist) {
		return errors.Wrap(err, baseErr.Error())
	}

	// 2. 若不存在或空间够，则获取新 block，否则直接复用老 block
	if errors.Is(err, ErrValueNotExist) || obl.GetBlockLength() < len(b) {
		bl, err = dm.pageManager.GetBlockBySize(len(b))
		if err != nil {
			return errors.Wrap(err, baseErr.Error())
		}
	} else {
		bl = obl
		useOldBlock = true
	}

	// 3. 写 block
	n, err := bl.Write(b)
	if err != nil {
		return errors.Wrap(err, baseErr.Error())
	}
	// 姑且这样用 block，用于释放多余 page
	defer bl.Close()

	if n != len(b) {
		return errors.Wrap(ErrUnexpectedEnd, baseErr.Error())
	}

	// 4. 更新 index
	err = dm.indexManager.SetPosi(ctx, key, bl.GetPosi())
	if err != nil {
		return errors.Wrap(err, baseErr.Error())
	}

	// 5. 释放无用的 block
	if !useOldBlock && obl != nil {
		obl.Free()
	}

	return nil
}

func (dm *DiskMap) Del(ctx context.Context, key string) {
	var baseErr = errors.New("del diskmap fail")
	bl, err := dm.getBlock(ctx, key)
	if err != nil {
		log.Println(errors.Wrap(err, baseErr.Error()))
		return
	}

	bl.Free()
}

func (dm *DiskMap) Range(ctx context.Context, f func(ctx context.Context, key string, value Value) bool) {
	// TODO range
}

func (dm *DiskMap) Encode(ctx context.Context) ([]byte, error) {
	// TODO encode all
	return nil, nil
}

func (dm *DiskMap) Decode(ctx context.Context, b []byte) (IMapStorage, error) {
	// TODO decode all
	return nil, nil
}

func (dm *DiskMap) getBlock(ctx context.Context, key string) (*Block, error) {
	po, ok := dm.indexManager.GetPosi(ctx, key)
	if !ok {
		return nil, ErrValueNotExist
	}

	return dm.pageManager.GetBlock(po)
}

func NewKeyIndex() *KeyIndex {
	return &KeyIndex{
		m: map[string]Posi{},
	}
}

type KeyIndex struct {
	mu sync.RWMutex

	m map[string]Posi
}

func (ki *KeyIndex) Has(ctx context.Context, key string) bool {
	ki.mu.RLock()
	defer ki.mu.Unlock()

	_, ok := ki.m[key]
	return ok
}

func (ki *KeyIndex) GetPosi(ctx context.Context, key string) (Posi, bool) {
	ki.mu.RLock()
	defer ki.mu.RUnlock()

	v, ok := ki.m[key]
	return v, ok
}

func (ki *KeyIndex) SetPosi(ctx context.Context, key string, posi Posi) error {
	ki.mu.Lock()
	defer ki.mu.RUnlock()

	ki.m[key] = posi

	return nil
}

func (ki *KeyIndex) Decode(b []byte) error {
	bm := MapStrBytes{}

	err := json.Unmarshal(b, &bm)
	if err != nil {
		return err
	}
	tm := make(map[string]Posi, len(bm))

	for k, bv := range bm {
		po, err := DecodePosi(*bv)
		if err != nil {
			return err
		}

		tm[k] = po
	}

	ki.m = tm

	return nil
}

func (ki *KeyIndex) Encode(ctx context.Context) ([]byte, error) {
	tm := make(MapStrBytes, len(ki.m))

	for k, p := range ki.m {
		b, err := p.Encode()
		if err != nil {
			return nil, err
		}

		tm[k] = ConvertBytesValue(b)
	}

	return json.Marshal(tm)
}

type Posi struct {
	PageNum PageNum
	Len     int
}

func DecodePosi(b []byte) (Posi, error) {
	return Posi{}.Decode(b)
}

func (p Posi) Decode(b []byte) (Posi, error) {
	if len(b) != 16 {
		return p, ErrWrongPosiLen
	}

	pn := binary.BigEndian.Uint64(b[0:8])
	Len := binary.BigEndian.Uint64(b[8:])
	p.PageNum = PageNum(pn)
	p.Len = int(Len)

	return p, nil
}

func (p *Posi) Encode() ([]byte, error) {
	b := make([]byte, 16)
	binary.BigEndian.PutUint64(b[0:8], uint64(p.PageNum))
	binary.BigEndian.PutUint64(b[8:16], uint64(p.Len))

	return b, nil
}

type IFile interface {
	Open(ctx context.Context, fpath string) os.File
}

type PageManager struct {
	mu sync.Mutex

	pagesBitMap *BitMap

	file *os.File

	PagesInfo

	baseGrowStep int
}

type PagesInfo struct {
	PagesNum int
	PageSize int
}

type PageNum uint64

type PageManagerOpt struct {
	PageSize     int
	BaseGrowStep int

	File *os.File // 数据文件

}

// 索引文件
type IndexFile struct {
	file os.File
}

func (idf *IndexFile) Read(b []byte) (n int, err error) {
	return idf.file.Read(b)
}

func (idf *IndexFile) Write(b []byte) (n int, err error) {
	return idf.file.Write(b)
}

func NewPageManager(opt PageManagerOpt) (*PageManager, error) {
	pm := &PageManager{
		file: opt.File,
	}

	pm.PageSize = opt.PageSize

	finfo, err := opt.File.Stat()
	if err != nil {
		return nil, err
	}

	fsize := finfo.Size()
	mod := fsize % int64(pm.PageSize)
	if mod != 0 {
		// 姑且用简单的余数校验，以后可以加 meta 信息，例如 checksum
		return nil, ErrFileBlockSize
	}

	pm.PagesNum = int(fsize) / pm.PageSize
	pm.pagesBitMap = NewBitMap(pm.PagesNum)

	return pm, nil
}

// GetBlockBySize 按所需大小获取 block
func (pm *PageManager) GetBlockBySize(size int) (*Block, error) {
	pages := (size-1)/pm.PageSize + 1

	var spage, epage int
	var ok bool

	err := TryTimes(func() error {
		spage, epage, ok = getContinuousPages(pm, pages)

		if !ok {
			pm.grow(pages)
			return errors.New("pages not enough")
		}

		return nil
	}, 5)
	if err != nil {
		return nil, err
	}

	// 这里不是原子的，有可能有并发问题
	for i := 0; i < epage-spage; i++ {
		pm.pagesBitMap.SetRange(spage, pages)
	}

	posi := Posi{
		PageNum: PageNum(spage),
		Len:     size,
	}

	return pm.GetBlock(posi)
}

func getContinuousPages(pm *PageManager, pages int) (start, end int, ok bool) {
	found := false
	startPage := 0

	for i := 0; i < pm.PagesNum; i++ {
		occupied := pm.pagesBitMap.Get(i)
		if occupied {
			startPage = i
			continue
		} else if i-startPage+1 == pages {
			found = true
			break
		}
	}

	if found {
		return startPage, startPage + pages, true
	}

	return 0, 0, false
}

// GetBlock 获取已有 block
func (pm *PageManager) GetBlock(po Posi) (*Block, error) {
	if pm.PagesNum < int(po.PageNum) {
		return nil, ErrInvalidPageNum
	}

	pagesNum := (po.Len-1)/int(pm.PageSize) + 1

	d := make([]byte, pagesNum*int(pm.PageSize))

	n, err := pm.file.ReadAt(d, int64(pm.getOffset(po.PageNum)))
	if err != nil {
		return nil, err
	}

	if n < po.Len {
		return nil, errors.New("read failed size shrinked")
	}

	pages := make([]*Page, pagesNum)
	for i := 0; i < pagesNum; i++ {
		pages[i] = &Page{}
	}

	pageNum := po.PageNum
	for _, p := range pages {
		p.pageNum = pageNum
		p.data = d[int(pageNum-po.PageNum)*int(pm.PageSize) : int(pageNum-po.PageNum+1)*int(pm.PageSize)]
		p.pageSize = int(pm.PageSize)
		pageNum++
	}

	b := NewBlock(int(pm.getPagesSize(pagesNum)), pages)
	b.filledLen = po.Len

	return b, nil
}

func (pm *PageManager) getPagesSize(pagesNum int) int64 {
	return int64(pm.PageSize) * int64(pagesNum)
}

func (pm *PageManager) getOffset(pn PageNum) int64 {
	return int64(pn) * int64(pm.PageSize)
}

// grow 增长多少个 page
func (pm *PageManager) grow(needPages int) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	growPages := needPages
	if pm.baseGrowStep > needPages {
		growPages = pm.baseGrowStep
	}

	off := pm.getOffset(PageNum(pm.PagesNum + growPages))

	tmpb := make([]byte, growPages*pm.PageSize)
	_, err := pm.file.WriteAt(tmpb, off)
	if err != nil {
		return err
	}

	pm.pagesBitMap.Grow(growPages)
	pm.PagesNum += growPages

	return nil
}

// Tidy 空间碎片清理
func (pm *PageManager) Tidy() {
	// todo
}

func (pm *PageManager) FreePage(pageNum PageNum) {
	pm.pagesBitMap.UnSet(int(pageNum))
}

// WritePage
func (pm *PageManager) writePage(p *Page) error {
	// pm.file.
	return nil

}

func (pm *PageManager) writeBlock(b *Block) error {
	// TODO
	return nil
}

func (pm *PageManager) GetPageByPageNum(pagenum PageNum) (*Page, error) {
	if pm.PagesNum < int(pagenum) {
		return nil, ErrInvalidPageNum
	}

	p := &Page{
		pageNum:  pagenum,
		pageSize: int(pm.PageSize),
		data:     make([]byte, pm.PageSize),
	}

	off := pagenum * PageNum(pm.PageSize)

	// todo use mmap
	_, err := pm.file.ReadAt(p.data, int64(off))
	if err != nil {
		return nil, err
	}

	return p, nil
}

type Page struct {
	pageNum  PageNum
	pageSize int
	dirty    bool

	// f os.File

	// TODO use mmap
	data []byte
}

func (p *Page) ReadAll() (b []byte, err error) {
	td := make([]byte, len(p.data))

	copy(td, p.data)

	return td, nil
}

func (p *Page) Read(d []byte) (n int, err error) {
	return copy(d, p.data), nil
}

func (p *Page) Write(d []byte) (n int, err error) {
	if p.pageSize < len(d) {
		return 0, ErrSpaceNotEnough
	}

	p.dirty = true

	// TODO write 和 read 要能够连续读写
	i := copy(p.data, d)

	return i, nil
}

func (p *Page) Len() int {
	return len(p.data)
}

func (p *Page) Close() (err error) {
	return nil
}

func NewBlock(blockLen int, pages []*Page) *Block {
	return &Block{
		blockLen: blockLen,
		pages:    pages,
	}
}

type Block struct {
	blockLen  int // 申请的空间大小
	filledLen int // 写入的空间大小

	pages []*Page

	pageManager *PageManager
}

func (b *Block) Read(tb []byte) (int, error) {
	maxLen := len(tb)
	nowLen := 0

	var err error
	var n int
	for _, p := range b.pages {
		if nowLen >= maxLen {
			return nowLen, nil
		}

		if p.Len() <= maxLen-nowLen {
			n, err = p.Read(tb[nowLen : nowLen+p.Len()])
		} else {
			n, err = p.Read(tb[nowLen:])
		}

		if err != nil {
			return 0, err
		}
		nowLen += n
	}

	return nowLen, nil
}

func (b *Block) ReadAll() ([]byte, error) {
	tb := make([]byte, b.filledLen)
	n, err := b.Read(tb)
	if err != nil {
		return nil, err
	}

	if n != len(tb) {
		return nil, ErrUnexpectedEnd
	}

	return tb, nil
}

func (b *Block) GetBlockLength() int {
	return b.blockLen
}

func (b *Block) GetFilledLength() int {
	return b.filledLen
}

// Free 完全释放该 block
func (b *Block) Free() {
	for _, p := range b.pages {
		b.pageManager.FreePage(p.pageNum)
	}
}

func (b *Block) GetPosi() Posi {
	p := Posi{}
	p.PageNum = b.pages[0].pageNum
	p.Len = b.filledLen

	return p
}

func (b *Block) Write(d []byte) (n int, err error) {
	if b.blockLen < len(d) {
		return 0, ErrSpaceNotEnough
	}

	idx := 0

	for _, p := range b.pages {
		lastPosi := p.pageSize + idx

		if len(d) < lastPosi {
			lastPosi = len(d)
		}

		n, err = p.Write(d[idx:lastPosi])
		if err != nil {
			return 0, err
		}

		if len(d) == lastPosi {
			break
		}

		idx += n
	}

	b.filledLen = n

	return
}

func (b *Block) Close() (err error) {
	err = b.pageManager.writeBlock(b)
	if err != nil {
		return err
	}

	for _, p := range b.pages {
		if !p.dirty {
			b.pageManager.FreePage(p.pageNum)
			continue
		}

		err := p.Close()
		if err != nil {
			return err
		}
	}

	return nil
}
