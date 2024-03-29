package imagine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDmaps(t *testing.T) {
	ctx := context.Background()

	d, err := NewDmaps(DmapsOption{Dir: "testdata"})
	assert.Nil(t, err)
	defer d.Close(ctx)

	// dm, err := getDmapsMeta("testdata")
	// assert.Nil(t, err)

	// mm, ok := dm.GetMapMeta("helloworld")
	// assert.True(t, ok)

	hd := d.MustGetMap(ctx, "helloworld")
	normalMapTest(ctx, normalMapOpt{
		Map: hd,
	}, t)

	ld := d.MustGetMap(ctx, "longtest")
	normalMapTest(ctx, normalMapOpt{
		Map: ld,
	}, t)

}
