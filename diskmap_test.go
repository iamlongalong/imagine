package imagine

import (
	"context"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPageManager(t *testing.T) {
	fp := "longtest.db"

	f, err := os.OpenFile(fp, os.O_RDWR|os.O_CREATE, 0644)
	assert.Nil(t, err)

	pm, err := NewPageManager(PageManagerOpt{
		PageSize: 512,
		File:     f,
	})

	assert.Nil(t, err)
	d1 := "hello longalong d1"
	ds1 := []byte(d1)

	b, err := pm.GetBlockBySize(len(ds1))
	assert.Nil(t, err)

	n, err := b.Write([]byte(d1))
	assert.Nil(t, err)
	assert.Equal(t, len(d1), n)

	d1r, err := b.ReadAll()
	assert.Nil(t, err)
	assert.EqualValues(t, d1, string(d1r))

	b.Close()

}

func TestDiskMap(t *testing.T) {
	ctx := context.Background()

	testDataDir := "./test/"
	os.Mkdir(testDataDir, 0755)

	baseName := testDataDir + "longtest." + string(genRandomBytes(6))

	dbfile := baseName + ".dm"
	f, err := os.OpenFile(dbfile, os.O_RDWR|os.O_CREATE, 0644)
	assert.Nil(t, err)
	defer f.Close()

	indexfile := baseName + ".dmx"
	idxf, err := os.OpenFile(indexfile, os.O_RDWR|os.O_CREATE, 0644)
	assert.Nil(t, err)
	defer idxf.Close()

	pagesize := 128
	dm, err := NewDiskMap(DiskMapOpt{
		DataFile:     f,
		IndexManager: idxf,
		PageSize:     pagesize,
	})
	assert.Nil(t, err)

	defer dm.Close(ctx)

	normalMapTest(ctx, normalMapOpt{
		PageSize: pagesize, Map: dm,
		IdxFilePath:      indexfile,
		DataFilePath:     dbfile,
		WithCloseAndOpen: true,
	}, t)

}

type normalMapOpt struct {
	PageSize         int
	Map              IMapStorage
	WithCloseAndOpen bool
	IdxFilePath      string
	DataFilePath     string
}

func normalMapTest(ctx context.Context, opt normalMapOpt, t *testing.T) {
	dm := opt.Map

	k1 := "longkey1"
	v1 := "longvalue1"
	bv1 := ConvertBytesValue([]byte(v1))

	k2 := "hahakey2"
	v2 := "hello world v2 hahaha"
	v2n := "this is not hello world, new test value 2 hahahah"
	bv2 := ConvertBytesValue([]byte(v2))
	bv2n := ConvertBytesValue([]byte(v2n))

	k3 := "xxkey3"

	k4 := "key4_for_replace"
	v4 := "value 4 for replace anohter val, such as v1"
	bv4 := ConvertBytesValue([]byte(v4))

	// has no value
	ok := dm.Has(ctx, k1)
	assert.False(t, ok)

	ok = dm.Has(ctx, k2)
	assert.False(t, ok)

	// get no value
	xv1, err := dm.Get(ctx, k1)
	assert.ErrorIs(t, err, ErrValueNotExist)
	assert.Nil(t, xv1)

	// set
	err = dm.Set(ctx, k1, bv1)
	assert.Nil(t, err)

	err = dm.Set(ctx, k2, bv2)
	assert.Nil(t, err)

	// get
	vv1, err := dm.Get(ctx, k1)
	assert.Nil(t, err)
	assert.EqualValues(t, bv1, vv1)

	vv2, err := dm.Get(ctx, k2)
	assert.Nil(t, err)
	assert.EqualValues(t, bv2, vv2)

	// has
	ok = dm.Has(ctx, k1)
	assert.True(t, ok)

	ok = dm.Has(ctx, k2)
	assert.True(t, ok)

	ok = dm.Has(ctx, k3)
	assert.False(t, ok)

	// set new value
	err = dm.Set(ctx, k2, bv2n)
	assert.Nil(t, err)

	nv2, err := dm.Get(ctx, k2)
	assert.Nil(t, err)
	assert.EqualValues(t, bv2n, nv2)

	// set bigger than space
	biggerValue := genRandomBytes(opt.PageSize + 20)
	biggerBytesValue := ConvertBytesValue(biggerValue)
	err = dm.Set(ctx, k2, biggerBytesValue)
	assert.Nil(t, err)

	biggerv2, err := dm.Get(ctx, k2)
	assert.Nil(t, err)
	assert.EqualValues(t, biggerBytesValue, biggerv2)

	// del and replace
	dm.Del(ctx, k3)

	dm.Del(ctx, k1)

	err = dm.Set(ctx, k4, bv4)
	assert.Nil(t, err)

	// TODO 去除特定实现的依赖
	if opt.WithCloseAndOpen {
		// close and reopen
		err = dm.Close(ctx)
		assert.Nil(t, err)

		nf, err := os.OpenFile(opt.DataFilePath, os.O_RDWR|os.O_CREATE, 0644)
		assert.Nil(t, err)
		defer nf.Close()

		nidxf, err := os.OpenFile(opt.IdxFilePath, os.O_RDWR|os.O_CREATE, 0644)
		assert.Nil(t, err)
		defer nidxf.Close()

		ndm, err := NewDiskMap(DiskMapOpt{
			DataFile:     nf,
			IndexManager: nidxf,
			PageSize:     opt.PageSize,
		})
		assert.Nil(t, err)

		ok = ndm.Has(ctx, k2)
		assert.True(t, ok)

		ok = ndm.Has(ctx, k3)
		assert.False(t, ok)

		ok = ndm.Has(ctx, k1)
		assert.False(t, ok)

		nnv4, err := ndm.Get(ctx, k4)
		assert.Nil(t, err)
		assert.EqualValues(t, bv4, nnv4)

		nbv2, err := ndm.Get(ctx, k2)
		assert.Nil(t, err)
		assert.EqualValues(t, biggerv2, nbv2)
	}

}

func genRandomBytes(l int) []byte {
	rand.Seed(time.Now().UnixNano())

	base := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_$"
	strBytes := make([]byte, l)

	// 前 52 个字母
	strBytes[0] = base[rand.Intn(52)]
	for i := 1; i < l; i++ {
		v := rand.Intn(len(base))
		strBytes[i] = base[v]
	}

	return strBytes
}
