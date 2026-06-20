package control

type WebSocket struct {
	Address string       `json:"address"`
	Last    *int64       `json:"last"`
	Primary bool         `json:"primary"`
	Traffic TrafficStats `json:"traffic"`
	Path    *PathStatus  `json:"path,omitempty"`
}

type WebInterface struct {
	Name          string       `json:"name"`
	Label         string       `json:"label,omitempty"`
	Status        string       `json:"status"`
	SenderAddress string       `json:"senderAddress"`
	DstAddress    string       `json:"dstAddress"`
	Last          *int64       `json:"last"`
	Primary       bool         `json:"primary"`
	Traffic       TrafficStats `json:"traffic"`
	Path          *PathStatus  `json:"path,omitempty"`
}

type TrafficCounters struct {
	RXPackets uint64 `json:"rxPackets"`
	RXBytes   uint64 `json:"rxBytes"`
	TXPackets uint64 `json:"txPackets"`
	TXBytes   uint64 `json:"txBytes"`
}

type TrafficStats struct {
	Data    TrafficCounters `json:"data"`
	Control TrafficCounters `json:"control"`
}

type PathStatus struct {
	LastSeen          *int64 `json:"lastSeen"`
	LastSuccess       *int64 `json:"lastSuccess"`
	SmoothedRTTMillis int64  `json:"rttMillis"`
	RTTVarianceMillis int64  `json:"rttVarianceMillis"`
	Failures          int    `json:"failures"`
	Eligible          bool   `json:"eligible"`
}

type ServerStatus struct {
	Type          string      `json:"type"`
	Version       string      `json:"version"`
	Description   string      `json:"description"`
	ListenAddress string      `json:"listenAddress"`
	DstAddress    string      `json:"dstAddress"`
	Sockets       []WebSocket `json:"sockets"`
}

type ClientStatus struct {
	Type          string         `json:"type"`
	Version       string         `json:"version"`
	Description   string         `json:"description"`
	ListenAddress string         `json:"listenAddress"`
	Interfaces    []WebInterface `json:"interfaces"`
}
