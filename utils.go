package imagine

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
)

func TryTimes(f func() error, times int) error {
	var err error
	for i := 0; i < times; i++ {
		err = f()
		if err != nil {
			continue
		}

		return nil
	}

	return err
}

// func GetCurrentBeijingTime(format string) string {
// 	return time.Now().In(time.FixedZone("CST", 8*3600)).Format(format)
// }

func TryTimesCtx(ctx context.Context, times int, f func(context.Context) error) error {
	retry := 0

	errs := BundleErr{}

	for {
		select {
		case <-ctx.Done():
			errs.Add(ctx.Err())

			return errs.Error()
		default:
		}

		retry++

		err := f(ctx)
		if err != nil {
			errs.Add(err)
			if retry > times {
				return errs.Error()
			}

			continue
		}

		return nil
	}
}

// BundleErr errors more than one, but return one, bundle them together
type BundleErr struct {
	errs []error
	mu   sync.Mutex
}

// Append add err anyway, do not care if nil or not
func (be *BundleErr) Append(e error) *BundleErr {
	be.mu.Lock()
	be.errs = append(be.errs, e)
	be.mu.Unlock()
	return be
}

// Add if err != nil, the err will be recored, if nil, will do nothing
func (be *BundleErr) Add(e error) *BundleErr {
	if e != nil {
		be.mu.Lock()
		be.errs = append(be.errs, e)
		be.mu.Unlock()
	}

	return be
}

// Error implement error interface
func (be *BundleErr) Error() error {
	if len(be.errs) == 0 {
		return nil
	}

	return &bundleErr{be}
}

// Errors get original errors
func (be *BundleErr) Errors() []error {
	be.mu.Lock()
	es := make([]error, len(be.errs))
	copy(es, be.errs)
	be.mu.Unlock()

	return es
}

// ClearNil clear nil err (which may be appended by Append func)
func (be *BundleErr) ClearNil() *BundleErr {
	be.mu.Lock()
	es := make([]error, 0, len(be.errs))

	for _, e := range be.errs {
		if e != nil {
			es = append(es, e)
		}
	}

	be.errs = es
	be.mu.Unlock()

	return be
}

func (be *BundleErr) Len() int {
	return len(be.errs)
}

func (be *BundleErr) Is(e error) bool {
	be.mu.Lock()
	defer be.mu.Unlock()

	for _, err := range be.errs {
		if errors.Is(e, err) {
			return true
		}
	}

	return false
}

type bundleErr struct {
	*BundleErr
}

func (b *bundleErr) Error() string {
	msg := ""

	b.mu.Lock()
	for _, e := range b.errs {
		if e == nil {
			continue
		}

		msg += e.Error() + ";"
	}
	b.mu.Unlock()

	return msg
}

func SafeRun(cb func(error), f func()) {
	defer func() {
		if err := recover(); err != nil {
			msg := GetCallerInfo(15)
			cb(fmt.Errorf("goroutine panic : %s", msg))
		}
	}()

	f()
}

func GetCallerInfo(depth int) string {
	callerInfo := ""

	for i := 3; i < depth+3; i++ {
		_, file, line, ok := runtime.Caller(i)
		if !ok {
			return callerInfo
		}

		callerInfo += fmt.Sprintf("%d : %s#%d \n", i-2, file, line)
	}

	return callerInfo
}

type GoPool interface {
	Run(context.Context, *sync.WaitGroup, func(error), func(context.Context)) error
}

func NewGoPool(size int) GoPool {
	p := GoroutinePool{}
	p.size = size

	p.allowChan = make(chan struct{}, size)

	return &p
}

type GoroutinePool struct {
	size int

	allowChan chan struct{}
}

func (p *GoroutinePool) Run(ctx context.Context, wg *sync.WaitGroup, cb func(error), f func(context.Context)) error {
	select {
	case <-ctx.Done():
		return ctx.Err()

	case p.allowChan <- struct{}{}:
		wg.Add(1)
		go SafeRun(cb, func() {
			defer wg.Done()

			f(ctx)
			<-p.allowChan
		})
	}

	return nil
}
