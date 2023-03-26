package imagine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

type valuserTests []struct {
	Ori    interface{}
	Expect interface{}
}

func TestBytesValuer(t *testing.T) {
	ts := valuserTests{
		{Ori: []byte("hello world longalong"), Expect: []byte("hello world longalong")},
		{Ori: []byte{1, 2, 3, 4, 5, 6, 101, 210}, Expect: []byte{1, 2, 3, 4, 5, 6, 101, 210}},
	}

	ctx := context.Background()

	bvf, err := GetValuer(BytesValuerName)
	assert.Nil(t, err)

	for _, xt := range ts {
		bv := ConvertBytesValue(xt.Ori.([]byte))
		tb, err := bv.Encode()
		assert.Nil(t, err)

		tbv, err := bvf(ctx, tb)
		assert.Nil(t, err)

		assert.EqualValues(t, ConvertBytesValue(xt.Expect.([]byte)), tbv)
	}

}

func TestJsonValuer(t *testing.T) {
	ts := valuserTests{
		{Ori: map[string]interface{}{}, Expect: map[string]interface{}{}},
		{Ori: map[string]interface{}{"hello": "world", "age": 18}, Expect: map[string]interface{}{"hello": "world", "age": float64(18)}}, // number type in json is float64
		{Ori: struct {
			Name string `json:"name"`
			Age  int    `json:"age"`
		}{Name: "longalong", Age: 18}, Expect: map[string]interface{}{"name": "longalong", "age": float64(18)}},
	}

	ctx := context.Background()

	jvf, err := GetValuer(JsonValuerName)
	assert.Nil(t, err)

	for _, xt := range ts {
		xv := ConvertJsonValue(xt.Ori)
		xb, err := xv.Encode()
		assert.Nil(t, err)

		iv, err := jvf(ctx, xb)
		assert.Nil(t, err)

		v, ok := iv.(Decoder)
		assert.True(t, ok)

		xm := map[string]interface{}{}

		err = v.Decode(&xm)
		assert.Nil(t, err)

		assert.EqualValues(t, xt.Expect, xm, 0)
	}

}
