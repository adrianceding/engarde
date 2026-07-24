package control

type WebSocket struct {
	Address string       `json:"address"`
	Traffic TrafficStats `json:"traffic"`
}

type WebInterface struct {
	Name                 string       `json:"name"`
	Label                string       `json:"label,omitempty"`
	Status               string       `json:"status"`
	SenderAddress        string       `json:"senderAddress"`
	DstAddress           string       `json:"dstAddress"`
	Last                 *int64       `json:"last"`
	Traffic              TrafficStats `json:"traffic"`
	QualityState         string       `json:"qualityState,omitempty"`
	RTTMillis            float64      `json:"rttMillis,omitempty"`
	JitterMillis         float64      `json:"jitterMillis,omitempty"`
	ScoreMillis          float64      `json:"scoreMillis,omitempty"`
	FailurePenaltyMillis float64      `json:"failurePenaltyMillis,omitempty"`
	ActiveFlows          int          `json:"activeFlows,omitempty"`
	ServerInstanceID     string       `json:"serverInstanceId,omitempty"`
}

type TrafficCounters struct {
	RXPackets      uint64 `json:"rxPackets"`
	RXBytes        uint64 `json:"rxBytes"`
	TXPackets      uint64 `json:"txPackets"`
	TXBytes        uint64 `json:"txBytes"`
	DropPackets    uint64 `json:"dropPackets"`
	DropBytes      uint64 `json:"dropBytes"`
	SkippedPackets uint64 `json:"skippedPackets"`
	SkippedBytes   uint64 `json:"skippedBytes"`
}

type TrafficStats struct {
	Data    TrafficCounters `json:"data"`
	Control TrafficCounters `json:"control"`
}

type TCPStreamStatus struct {
	ID          string `json:"id"`
	Version     uint8  `json:"protocolVersion"`
	Destination string `json:"destination"`
	Carriers    int    `json:"carriers"`
	State       string `json:"state"`
	Recoverable bool   `json:"recoverable,omitempty"`
	Generation  uint64 `json:"carrierGeneration,omitempty"`
}

type ServerStatus struct {
	Type            string            `json:"type"`
	Version         string            `json:"version"`
	Description     string            `json:"description"`
	ListenAddress   string            `json:"listenAddress"`
	PeerAuthEnabled bool              `json:"peerAuthEnabled,omitempty"`
	Sockets         []WebSocket       `json:"sockets"`
	TCPStreams      []TCPStreamStatus `json:"tcpStreams,omitempty"`
	Streams         int               `json:"streams,omitempty"`
	Carriers        int               `json:"carriers,omitempty"`
	Sessions        int               `json:"sessions,omitempty"`
	CarrierMode     string            `json:"carrierMode,omitempty"`
	Recovering      int               `json:"recovering,omitempty"`
}

type ClientStatus struct {
	Type                string         `json:"type"`
	Version             string         `json:"version"`
	Description         string         `json:"description"`
	ListenAddress       string         `json:"listenAddress"`
	FrontendAuthEnabled bool           `json:"frontendAuthEnabled,omitempty"`
	PeerAuthEnabled     bool           `json:"peerAuthEnabled,omitempty"`
	Interfaces          []WebInterface `json:"interfaces"`
	Streams             int            `json:"streams,omitempty"`
	Carriers            int            `json:"carriers,omitempty"`
	Sessions            int            `json:"sessions,omitempty"`
	CarrierMode         string         `json:"carrierMode,omitempty"`
	Recovering          int            `json:"recovering,omitempty"`
}
