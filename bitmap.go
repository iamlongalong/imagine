package imagine

import "sync"

func NewBitMap(size int) *BitMap {
	return &BitMap{
		data: make([]byte, (size-1)/8+1),
		l:    size,
	}
}

type BitMap struct {
	mu   sync.RWMutex
	l    int
	data []byte
}

func (b *BitMap) Get(pos int) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.data[pos/8]&(1<<(pos%8)) == (1 << (pos % 8))
}

func (b *BitMap) Set(pos int) {
	b.mu.Lock()
	b.data[pos/8] |= (1 << (pos % 8))
	b.mu.Unlock()
}

func (b *BitMap) SetIfEmpty(pos int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	// not empty
	if b.data[pos/8]&(1<<(pos%8)) == (1 << (pos % 8)) {
		return false
	}

	b.data[pos/8] |= (1 << (pos % 8))
	return true
}

func (b *BitMap) Sets(poss ...int) {
	b.mu.Lock()
	for _, pos := range poss {
		b.data[pos/8] |= (1 << (pos % 8))
	}
	b.mu.Unlock()
}

func (b *BitMap) SetRange(spos int, length int) {
	b.mu.Lock()
	for i := 0; i < length; i++ {
		pos := (spos + i)
		b.data[pos/8] |= (1 << (pos % 8))
	}
	b.mu.Unlock()
}

func (b *BitMap) SetAll() {
	b.mu.Lock()
	for i := 0; i < len(b.data); i++ {
		b.data[i] = 255
	}
	b.mu.Unlock()
}

func (b *BitMap) UnSet(pos int) {
	b.mu.Lock()
	b.data[pos/8] &= ^(1 << (pos % 8))
	b.mu.Unlock()
}

func (b *BitMap) UnSets(poss ...int) {
	b.mu.Lock()
	for _, pos := range poss {
		b.data[pos/8] &= ^(1 << (pos % 8))
	}
	b.mu.Unlock()
}

func (b *BitMap) UnSetRange(spos int, length int) {
	b.mu.Lock()
	for i := 0; i < length; i++ {
		pos := (spos + i)
		b.data[pos/8] &= ^(1 << (pos % 8))
	}
	b.mu.Unlock()
}

func (b *BitMap) Clear() {
	b.mu.Lock()
	for i := 0; i < len(b.data); i++ {
		b.data[i] = 0
	}
	b.mu.Unlock()
}

func (b *BitMap) Grow(size int) {
	b.mu.Lock()
	d := make([]byte, (b.l+size-1)/8+1)

	copy(d, b.data)

	b.data = d
	b.l += size
	b.mu.Unlock()
}
