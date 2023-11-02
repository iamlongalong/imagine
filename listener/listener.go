package listener

import "imagine"

type ListenOptions struct {
	OnSet func(key string, oi, ni imagine.Valuer)
	OnDel func(key string, oi imagine.Valuer)
}

type Listener interface {
	Listen(keys []string, options ListenOptions) error
}

type MListener struct{}

func (ml *MListener) Listen(keys []string, options ListenOptions) error {
	return nil
}
