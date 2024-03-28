package tcp

type Gossip struct {
}

func (g *Gossip) processHello(msgBytes *[]byte) {
}

// processRequest
func (g *Gossip) processRequest(msgBytes *[]byte) {
}

// processResponse
func (g *Gossip) processResponse(msgBytes *[]byte) {
}

func isGossipMessage(msgType byte) bool {
	return msgType == PACKET_GOSSIP_REQ || msgType == PACKET_GOSSIP_RES || msgType == PACKET_GOSSIP_HELLO
}
