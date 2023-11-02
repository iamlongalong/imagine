package merger

import (
	"github.com/imdario/mergo"
)

// Merger 通用 merger, 需要实现 undo 能力
type Merger interface {
	Merge(dst interface{}, src interface{}) // 合并
	Undo(dst interface{}, al AheadLog)      // 取消合并
}

// Merge
// 接收 interface 使用反射 和 硬编码 的性能差异有多少呢？ 便利性又如何呢？
// TODO 需要做 benchmark 和 demos
func Merge(dst interface{}, src interface{}) error {
	return mergo.Merge(dst, src)
}

// TODO
func Undo(dst interface{}, al AheadLog) error {
	// 1. decode log to struct (undo log)
	// al.LogContent

	// 2. merge to dst
	return nil
}
