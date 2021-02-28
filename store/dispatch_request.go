package store

type DispatchRequest struct {
	topic                     string
	queueId                   int32
	commitLogOffset           int64
	msgSize                   int32
	tagsCode                  int64
	storeTimestamp            int64
	consumeQueueOffset        int64
	keys                      string
	success                   bool
	uniqKey                   string
	sysFlag                   int32
	preparedTransactionOffset int64
	propertiesMap             map[string]string
	bitMap                    []byte
}
