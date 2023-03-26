package registry

import (
	"sync"

	"github.com/pkg/errors"
)

var dfRegistryRegistry = newRegistry()

var dfregistry, _ = NewRegistry("")

// MustGetRegistry get registry or create one
func MustGetRegistry(name string) IRegistry {
	i, ok := dfRegistryRegistry.Get(name)
	if !ok {
		i, _ = NewRegistry(name)
		// dfRegistryRegistry.Registry(name)
	}

	return i.(IRegistry)
}

// GetRegistry get registry by name
func GetRegistry(name string) (IRegistry, bool) {
	v, ok := dfRegistryRegistry.Get(name)
	if ok {
		return v.(IRegistry), ok
	}

	return nil, ok
}

type IRegistry interface {
	Registry(Namer) error
	Get(name string) (interface{}, bool)
}

// Registry register to default Registry
func Registry(n Namer) error {
	return dfregistry.Registry(n)
}

// Registry get from default Registry
func Get(name string) (interface{}, bool) {
	return dfregistry.Get(name)
}

func newRegistry() IRegistry {
	return &UniversalRegistry{}
}

// NewRegistry create a new registry
// if with namespace, the registry will be saved, then you can use GetRegistry(name) to get the registry
func NewRegistry(namespace ...string) (IRegistry, error) {
	r := newRegistry()

	if len(namespace) > 0 {
		err := dfRegistryRegistry.Registry(StrNamer(namespace[0]))
		if err != nil {
			return nil, errors.Errorf("NewRegistry fail with same namespace : %s", namespace[0])
		}
	}

	return r, nil
}

// =========== universal registry ===========

type UniversalRegistry struct {
	m sync.Map
}

func (ur *UniversalRegistry) Registry(v Namer) error {
	_, l := ur.m.LoadOrStore(v.Name(), v)
	if l {
		return errors.New("duplicate registry")
	}

	return nil
}

func (ur *UniversalRegistry) Get(name string) (interface{}, bool) {
	return ur.m.Load(name)
}

// ========== namer =========

type Namer interface {
	Name() string
}

type StrNamer string

func (sn StrNamer) Name() string {
	return string(sn)
}

type FuncNamer func() string

func (fn FuncNamer) Name() string {
	return fn()
}
