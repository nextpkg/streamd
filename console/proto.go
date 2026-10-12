package console

// streamInfo 单个流的信息
type streamInfo struct {
	Key           string `json:"key"`
	Pts           uint32 `json:"pts"`
	Duration      int64  `json:"duration"`
	VideoReceived uint64 `json:"videoReceived"`
	VideoSpeed    uint64 `json:"videoSpeed"`
	AudioReceived uint64 `json:"audioReceived"`
	AudioSpeed    uint64 `json:"audioSpeed"`
}

// publisherInfo 发布者的详细信息
type publisherInfo struct {
	Count int          `json:"count"`
	List  []streamInfo `json:"list"`
}

// subscriberInfo 订阅者的详细信息
type subscriberInfo struct {
	Count int          `json:"count"`
	List  []streamInfo `json:"list"`
}

// subscriberOnline 订阅者的在线数
type subscriberOnline struct {
	Count   int   `json:"count"`
	Checkin int64 `json:"checkin"`
}

// publisherKeys 发布者列表
type publisherKeys struct {
	Count int      `json:"count"`
	List  []string `json:"list"`
}
