package imagine

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMemMapBytesVal(t *testing.T) {
	ctx := context.Background()

	mm := NewMemMap(MemMapOpt{})

	k1 := "longtest1"
	ov1 := []byte("hello world 001")
	v1 := ConvertBytesValue(ov1)

	bv1, err := v1.Encode()
	assert.Nil(t, err)
	assert.Equal(t, ov1, bv1)

	k2 := "longtest2"
	v2 := ConvertBytesValue([]byte("hello world 002"))

	k3 := "longtest3"
	k4 := "longtest4"

	mm.Set(ctx, k1, v1)
	mm.Set(ctx, k2, v2)

	// get
	tv1, err := mm.Get(ctx, k1)
	assert.Nil(t, err)
	assert.Equal(t, v1, tv1)

	tv2, err := mm.Get(ctx, k2)
	assert.Nil(t, err)
	assert.Equal(t, v2, tv2)

	tv3, err := mm.Get(ctx, k3)
	assert.ErrorIs(t, err, ErrValueNotExist)
	assert.Nil(t, tv3)

	// has
	ok := mm.Has(ctx, k2)
	assert.True(t, ok)
	ok = mm.Has(ctx, k3)
	assert.False(t, ok)
	ok = mm.Has(ctx, k4)
	assert.False(t, ok)

	// encode decode
	b, err := mm.Encode(ctx)
	assert.Nil(t, err)
	fmt.Printf("%s\n", b)
	fmt.Println("")

	var tm IMapStorage = NewMemMap(MemMapOpt{})
	tm, err = tm.Decode(ctx, b)
	assert.Nil(t, err)

	tmv1, err := tm.Get(ctx, k1)
	assert.Nil(t, err)

	tmb1b, err := tmv1.Encode()
	assert.Nil(t, err)
	assert.Equal(t, ov1, tmb1b)

	// del
	tm.Del(ctx, k1)
	ok = tm.Has(ctx, k1)
	assert.False(t, ok)
	ok = tm.Has(ctx, k2)
	assert.True(t, ok)

	tm.Del(ctx, k4)
	ok = tm.Has(ctx, k1)
	assert.False(t, ok)
	ok = tm.Has(ctx, k2)
	assert.True(t, ok)
	ok = tm.Has(ctx, k4)
	assert.False(t, ok)

}
