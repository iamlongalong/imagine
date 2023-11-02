package compressor

import (
	"imagine"
	"imagine/registry"
	"io"

	"github.com/klauspost/compress/s2"
	"github.com/pkg/errors"
)

type Compressor interface {
	Name() string

	Compress(src io.Reader, dst io.Writer) error
	Decompress(src io.Reader, dst io.Writer) error
}

var registryName = "_compressors"

var (
	Snappy2Name = "_s2"
	FakeName    = "_fake"
)

var (
	s2compressor   Compressor = &Snappy2Compressor{}
	fakecompressor Compressor = &FakeCompressor{}
)

var r = registry.MustGetRegistry(registryName)

func init() {
	berr := imagine.BundleErr{}

	berr.
		Add(r.Registry(s2compressor)).
		Add(r.Registry(fakecompressor))

	if berr.Error() != nil {
		panic(errors.Wrap(berr.Error(), "registry compressor fail"))
	}
}

// ====== snappy2 ========

type Snappy2Compressor struct{}

func (sc *Snappy2Compressor) Name() string {
	return Snappy2Name
}

func (sc *Snappy2Compressor) Compress(src io.Reader, dst io.Writer) error {
	enc := s2.NewWriter(dst)
	_, err := io.Copy(enc, src)
	if err != nil {
		enc.Close()
		return err
	}

	return enc.Close()
}

func (sc *Snappy2Compressor) Decompress(src io.Reader, dst io.Writer) error {
	dec := s2.NewReader(src)
	_, err := io.Copy(dst, dec)
	return err
}

// ========= fake ==========

type FakeCompressor struct{}

func (fc *FakeCompressor) Name() string {
	return FakeName
}
func (fc *FakeCompressor) Compress(src io.Reader, dst io.Writer) error {
	io.Copy(dst, src)
	return nil
}
func (fc *FakeCompressor) Decompress(src io.Reader, dst io.Writer) error {
	io.Copy(dst, src)
	return nil
}

func GetCompressor(name string) Compressor {
	v, ok := r.Get(name)
	if ok {
		return v.(Compressor)
	}

	return fakecompressor
}
