package imagine

import (
	"context"
	"errors"
	"os"
)

var (
	ErrSpaceNotEnough = errors.New("space not enough")
)

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
func (pm *PageManager) GetBlock(startPageNum PageNum, size int) (*Block, error) {
	pagesNum := (size-1)/int(pm.pageSize) + 1

	d := make([]byte, pagesNum*int(pm.pageSize))

	n, err := pm.file.ReadAt(d, int64(pm.getOffset(startPageNum)))
	if err != nil {
		return nil, err
	}

	if n < size {
		return nil, errors.New("read failed size shrinked")
	}

	pages := make([]*Page, pagesNum)

	pageNum := startPageNum
	for _, p := range pages {
		p.pageNum = pageNum
		p.data = d[int(pageNum-startPageNum)*int(pm.pageSize) : int(pageNum-startPageNum+1)*int(pm.pageSize)]
		p.pageSize = int(pm.pageSize)
		pageNum++
	}

	b := NewBlock(int(pm.getPagesSize(pagesNum)), pages)

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

	data []byte // use mmap
}

func (p *Page) Write(d []byte) (n int, err error) {
	if p.pageSize < len(d) {
		return 0, ErrSpaceNotEnough
	}

	p.dirty = true

	i := copy(p.data, d)

	return i, nil
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
