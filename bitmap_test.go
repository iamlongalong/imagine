package imagine

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type BitMapLenTest struct {
	Val       interface{}
	ExpectLen int
}

func TestBitMapLen(t *testing.T) {
	ts := []BitMapLenTest{
		{Val: NewBitMap(20).data, ExpectLen: 3},
		{Val: NewBitMap(100).data, ExpectLen: 13},
		{Val: NewBitMap(200).data, ExpectLen: 25},
		{Val: NewBitMap(612).data, ExpectLen: 77},
		{Val: NewBitMap(1995).data, ExpectLen: 250},
	}

	for _, bmt := range ts {
		assert.Len(t, bmt.Val, bmt.ExpectLen)
	}
}

type BitMapFuncTest struct {
	F          func(b *BitMap)
	ExpectFunc func(b *BitMap)
}

func TestBitMapGetSetGrow(t *testing.T) {
	b := NewBitMap(20)

	bs := []BitMapFuncTest{
		{F: func(b *BitMap) {
			b.Set(10)
			b.Set(18)
		}, ExpectFunc: func(b *BitMap) {
			assert.Equal(t, true, b.Get(10))
			assert.Equal(t, true, b.Get(18))

			assert.Equal(t, false, b.Get(12))
			assert.Equal(t, false, b.Get(1))
			assert.Equal(t, false, b.Get(20))
		}},
		{
			F: func(b *BitMap) {
				b.Set(12)
				b.UnSet(18)
			}, ExpectFunc: func(b *BitMap) {
				assert.Equal(t, true, b.Get(12))
				assert.Equal(t, false, b.Get(18))
				assert.Equal(t, false, b.Get(1))
			},
		}, {
			F: func(b *BitMap) {
				b.Grow(10)
			}, ExpectFunc: func(b *BitMap) {
				assert.Equal(t, false, b.Get(25))
				assert.Equal(t, false, b.Get(30))

				assert.Len(t, b.data, 4)
			},
		},
	}

	for _, bt := range bs {
		bt.F(b)
		bt.ExpectFunc(b)
	}

}
