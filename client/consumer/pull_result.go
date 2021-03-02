package consumer

import "container/list"

type PullResult struct {
	PullStatus      PullStatus
	MsgFoundList    *list.List
	NextBeginOffset int64
	MinOffset       int64
	MaxOffset       int64
}
