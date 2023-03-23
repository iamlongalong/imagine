package imagine

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"log"
	"os"
	"sync"

	"github.com/pkg/errors"
)

var (
	ErrSpaceNotEnough = errors.New("space not enough")
	ErrWrongPosiLen   = errors.New("posi []byte len must be 16")

	ErrUnexpectedEnd             = errors.New("read unexpected end")
	dm               IMapStorage = &DiskMap{}
)

type DiskMapOpt struct {
	ValueFunc func(ctx context.Context, b []byte) (Value, error)
}

func NewDiskMap(opt DiskMapOpt) IMapStorage {
	if opt.ValueFunc == nil {
		opt.ValueFunc = DecodeBytesValue
	}

	return &DiskMap{
		valueFunc: opt.ValueFunc,
	}
}

type DiskMap struct {
	indexManager KeyIndex

	pageManager PageManager

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
	if err != nil {
		return errors.Wrap(err, baseErr.Error())
	}

	// 2. 若不够，则获取新 block
	if obl.GetBlockLength() < len(b) {
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
	if !useOldBlock {
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
	pagesBitMap *BitMap
	pageSize    int

	file os.File
}

type PageNum uint64

type PageManagerOpt struct {
	PageSize int
	Index    IndexFile
	File     os.File // 数据文件
}

// 索引文件
type IndexFile struct {
	file os.File
}

func NewPageManager(opt PageManagerOpt) *PageManager {
	return &PageManager{
		pageSize:    opt.PageSize,
		pagesBitMap: NewBitMap(10),
	}

}

// GetBlockBySize 按所需大小获取 block
func (pm *PageManager) GetBlockBySize(size int) (*Block, error) {
	return nil, nil
}

// GetBlock 获取已有 block
func (pm *PageManager) GetBlock(po Posi) (*Block, error) {
	pagesNum := (po.Len-1)/int(pm.pageSize) + 1

	d := make([]byte, pagesNum*int(pm.pageSize))

	n, err := pm.file.ReadAt(d, int64(pm.getOffset(po.PageNum)))
	if err != nil {
		return nil, err
	}

	if n < po.Len {
		return nil, errors.New("read failed size shrinked")
	}

	pages := make([]*Page, pagesNum)

	pageNum := po.PageNum
	for _, p := range pages {
		p.pageNum = pageNum
		p.data = d[int(pageNum-po.PageNum)*int(pm.pageSize) : int(pageNum-po.PageNum+1)*int(pm.pageSize)]
		p.pageSize = int(pm.pageSize)
		pageNum++
	}

	b := NewBlock(int(pm.getPagesSize(pagesNum)), pages)
	b.filledLen = po.Len

	return b, nil
}

func (pm *PageManager) getPagesSize(pagesNum int) int64 {
	return int64(pm.pageSize) * int64(pagesNum)
}

func (pm *PageManager) getOffset(pn PageNum) int64 {
	return int64(pn) * int64(pm.pageSize)
}

// grow 增长多少个 page
func (pm *PageManager) grow(pages int) error {
	// todo
	return nil
}

// Tidy 空间碎片清理
func (pm *PageManager) Tidy() {
	// todo
}

func (pm *PageManager) FreePage(pageNum PageNum) {
	pm.pagesBitMap.UnSet(int(pageNum))
}

func (pm *PageManager) GetPageByPageNum(pagenum PageNum) (*Page, error) {
	p := &Page{
		pageNum:  pagenum,
		pageSize: int(pm.pageSize),
		data:     make([]byte, pm.pageSize),
	}

	off := pagenum * PageNum(pm.pageSize)

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
