package imagine

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"
)

func NewBitMap(size int) *BitMap {
	return &BitMap{
		data: make([]byte, (size-1)/8+1),
		l:    size,
	}
}

// NewBitMapWithData
// NewBitMapWithData([]byte(10,101)) got a BitMap with lenth 16
// NewBitMapWithData([]byte(10,101), 10) got a BitMap with lenth 10
// NewBitMapWithData([]byte(10,101), 20) got a BitMap with lenth 20, (17-20 will be false)
func NewBitMapWithData(bs []byte, expectedSize ...int) *BitMap {
	size := len(bs) * 8
	if len(expectedSize) > 0 {
		size = expectedSize[0]
	}

	td := make([]byte, (size-1)/8+1)
	copy(td, bs)
	tb := &BitMap{
		data: td,
		l:    size,
	}

	if size/8 < len(bs) {
		ob := &BitMap{l: len(bs) * 8, data: bs}
		off := size - size%8
		for ; off < size; off++ {
			tb.setTo(off, ob.get(off))
		}
	}

	return tb
}

type BitMap struct {
	mu   sync.RWMutex
	l    int
	data []byte
}

// Merge merge a new BitMap B to the origin BitMap A
// off=10 refer to the origin BitMap A's offset, which means A's position 11 = B's position 1
func (b *BitMap) Merge(bm *BitMap, off int, length ...int) {
	if off > bm.l {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	bm.mu.Lock()
	defer bm.mu.Unlock()

	bl := bm.l
	if len(length) > 0 {
		bl = length[0]
	}

	// 希望 merge 的长度大于总长度，则仅 merge 总长度
	if bl > bm.l {
		bl = bm.l
	}

	// 被 merge 不够长，则增长到需要的长度
	if b.l < bl+off {
		b.grow(bl + off - b.l)
	}

	// 合并
	for i := 0; i < bl; i++ {
		b.setTo(off+i, bm.get(i))
	}
}

func (b *BitMap) get(pos int) bool {
	return b.data[pos/8]&(1<<(pos%8)) == (1 << (pos % 8))
}

func (b *BitMap) Get(pos int) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.get(pos)
}

func (b *BitMap) setTo(pos int, ok bool) {
	if ok {
		b.data[pos/8] |= (1 << (pos % 8))
	} else {
		b.data[pos/8] &= ^(1 << (pos % 8))
	}
}

func (b *BitMap) Set(pos int) {
	b.mu.Lock()
	b.set(pos)
	b.mu.Unlock()
}

func (b *BitMap) set(pos int) {
	b.data[pos/8] |= (1 << (pos % 8))
}

func (b *BitMap) SetIfEmpty(pos int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.setIfEmpty(pos)
}

func (b *BitMap) setIfEmpty(pos int) bool {
	// not empty
	if b.data[pos/8]&(1<<(pos%8)) == (1 << (pos % 8)) {
		return false
	}

	b.data[pos/8] |= (1 << (pos % 8))
	return true
}

func (b *BitMap) Sets(poss ...int) {
	b.mu.Lock()
	b.sets(poss...)
	b.mu.Unlock()
}

func (b *BitMap) sets(poss ...int) {
	for _, pos := range poss {
		b.data[pos/8] |= (1 << (pos % 8))
	}
}

func (b *BitMap) SetRange(spos int, length int) {
	b.mu.Lock()
	b.setRange(spos, length)
	b.mu.Unlock()
}

func (b *BitMap) setRange(spos int, length int) {
	for i := 0; i < length; i++ {
		pos := (spos + i)
		b.data[pos/8] |= (1 << (pos % 8))
	}
}

func (b *BitMap) SetAll() {
	b.mu.Lock()
	b.setAll()
	b.mu.Unlock()
}
func (b *BitMap) setAll() {
	for i := 0; i < len(b.data); i++ {
		b.data[i] = 255
	}
}

func (b *BitMap) UnSet(pos int) {
	b.mu.Lock()
	b.unSet(pos)
	b.mu.Unlock()
}

func (b *BitMap) unSet(pos int) {
	b.data[pos/8] &= ^(1 << (pos % 8))
}

func (b *BitMap) UnSets(poss ...int) {
	b.mu.Lock()
	b.unSets(poss...)
	b.mu.Unlock()
}

func (b *BitMap) unSets(poss ...int) {
	for _, pos := range poss {
		b.data[pos/8] &= ^(1 << (pos % 8))
	}
}

func (b *BitMap) UnSetRange(spos int, length int) {
	b.mu.Lock()
	b.unSetRange(spos, length)
	b.mu.Unlock()
}

func (b *BitMap) unSetRange(spos int, length int) {
	for i := 0; i < length; i++ {
		pos := (spos + i)
		b.data[pos/8] &= ^(1 << (pos % 8))
	}
}

func (b *BitMap) Clear() {
	b.mu.Lock()
	b.clear()
	b.mu.Unlock()
}

func (b *BitMap) clear() {
	for i := 0; i < len(b.data); i++ {
		b.data[i] = 0
	}
}

func (b *BitMap) Clone(off int, length int) *BitMap {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.clone(off, length)
}

func (b *BitMap) clone(off int, length int) *BitMap {
	nb := NewBitMap(length)

	if off > b.l { // 超出原有的都默认为空
		return nb
	}

	for i := off; i < b.l; i++ {
		nb.setTo(i-off, b.get(i))
	}

	return nb
}

func (b *BitMap) Encode() (length int, tb []byte) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	tb = make([]byte, (b.l-1)/8+1)
	copy(tb, b.data)

	return b.l, tb
}

func (b *BitMap) Grow(size int) {
	b.mu.Lock()
	b.grow(size)
	b.mu.Unlock()
}

func (b *BitMap) grow(size int) {
	d := make([]byte, (b.l+size-1)/8+1)

	copy(d, b.data)

	b.data = d
	b.l += size
}

func WriteBitMapToFile(ctx context.Context, b *BitMap, f *os.File) error {
	l, d := b.Encode()
	lb := make([]byte, 8)
	binary.BigEndian.PutUint64(lb, uint64(l))

	_, err := f.WriteAt(lb, 0)
	if err != nil {
		return err
	}

	_, err = f.WriteAt(d, 8)
	return err
}

func ReadBitMapFromFile(ctx context.Context, f *os.File) (*BitMap, error) {
	lb := make([]byte, 8)
	_, err := f.ReadAt(lb, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}

	l := binary.BigEndian.Uint64(lb)

	var tb []byte

	if l == 0 {
		tb = make([]byte, 0)
	} else {
		tb = make([]byte, (l-1)/8+1)
	}

	_, err = f.ReadAt(tb, 8)
	if err != nil {
		return nil, err
	}

	return NewBitMapWithData(tb, int(l)), nil
}
