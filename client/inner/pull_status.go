package inner

type PullStatus int

const (
	Found PullStatus = iota
	NoNewMsg
	NoMatchedMsg
	OffsetIllegal
)
