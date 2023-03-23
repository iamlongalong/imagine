package imagine

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDiskMapBytes(t *testing.T) {
	// dm := NewDiskMap(DiskMapOpt{})
	// dm.Set()
}

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
