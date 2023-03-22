package imagine

func NewBitMap(size int) *BitMap {
	return &BitMap{
		data: make([]byte, (size-1)/8+1),
		l:    size,
	}
}

type BitMap struct {
	l    int
	data []byte
}

func (b *BitMap) Get(pos int) bool {
	return b.data[pos/8]&(1<<(pos%8)) == (1 << (pos % 8))
}

func (b *BitMap) Set(pos int) {
	b.data[pos/8] |= (1 << (pos % 8))
}

func (b *BitMap) SetAll() {
	for i := 0; i < len(b.data); i++ {
		b.data[i] = 255
	}
}

func (b *BitMap) UnSet(pos int) {
	b.data[pos/8] &= ^(1 << (pos % 8))
}

func (b *BitMap) Clear() {
	for i := 0; i < len(b.data); i++ {
		b.data[i] = 0
	}
}

func (b *BitMap) Grow(size int) {

	d := make([]byte, (b.l+size-1)/8+1)

	copy(d, b.data)

	b.data = d
	b.l += size
}
