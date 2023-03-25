package imagine

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestImageineMap(t *testing.T) {
	ctx := context.Background()

	testDataDir := "./test/"
	os.Mkdir(testDataDir, 0755)

	baseName := testDataDir + "longtest." + string(genRandomBytes(6))
	dbfile := baseName + ".dm"
	indexfile := baseName + ".dmx"
	dmfile := baseName + ".dmb"

	pagesize := 128

	dmopt, err := BuildDiskMapOpt(dbfile, indexfile, dmfile, pagesize, DecodeBytesValue)
	assert.Nil(t, err)

	opt := ImagineOption{
		MemMapOpt:  MemMapOpt{ValueFunc: DecodeBytesValue},
		DiskMapOpt: dmopt,
	}

	im, err := NewImagineMap(opt)
	assert.Nil(t, err)
	assert.NotNil(t, im)
	defer im.Close(ctx)

	normalMapTest(ctx, normalMapOpt{
		PageSize: pagesize, Map: im,
		IdxFilePath:      indexfile,
		DataFilePath:     dbfile,
		WithCloseAndOpen: false,
	}, t)
}
