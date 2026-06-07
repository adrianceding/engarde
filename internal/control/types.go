package control

type WebSocket struct {
	Address string `json:"address"`
	Last    *int64 `json:"last"`
}

type WebInterface struct {
	Name          string `json:"name"`
	Label         string `json:"label,omitempty"`
	Status        string `json:"status"`
	SenderAddress string `json:"senderAddress"`
	DstAddress    string `json:"dstAddress"`
	Last          *int64 `json:"last"`
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
