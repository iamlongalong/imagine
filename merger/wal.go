package merger

type IWal interface {
	WriteLog(l AheadLog)

	GetUnCommits()
}

type aLogType int

const (
	LogTypeMergeStart aLogType = iota + 1
	LogTypeMergeCommit
	LogTypeMergeRevert

	LogTypeAddIndexStart
)

type AheadLog struct {
	LogType    aLogType // log 类型
	LogContent []byte
}
