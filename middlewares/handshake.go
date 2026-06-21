package middlewares

import (
	"encoding/json"
	"fmt"

	"github.com/nats-io/nats.go"
)

var (
	HandshakeMissingHeaderHandshakeErr = fmt.Errorf("Handshake-mw: Missing handshake\n")
	HandshakeHeaderParseErr            = fmt.Errorf("Handshake-mw: error parsing handshake\n")
	HandshakeInvalidTokenValuesErr     = fmt.Errorf("Handshake-mw: id does not match expiration\n")
)

type handshake struct {
	factory HandshakeFactory
}

type Handshake interface {
	Validate() error
}

type HandshakeFactory func() Handshake

func NewHandshakeMiddleware(factory HandshakeFactory) Middleware {
	return &handshake{factory: factory}
}

func (mw handshake) Exec(next func(msg *nats.Msg)) func(msg *nats.Msg) {
	return func(msg *nats.Msg) {
		hsStr := msg.Header.Get("Handshake")
		if hsStr == "" {
			replyError(msg, HandshakeMissingHeaderHandshakeErr)
			return
		}

		hs := mw.factory()
		err := json.Unmarshal([]byte(hsStr), &hs)
		if err != nil {
			replyError(msg, fmt.Errorf("%wErr: %s", HandshakeHeaderParseErr, err.Error()))
			return
		}

		if hs.Validate() != nil {
			replyError(msg, HandshakeInvalidTokenValuesErr)
		}

		next(msg)
	}
}

func replyError(msg *nats.Msg, errMsg error) {
	if msg.Reply == "" {
		return // fire-and-forget message, nothing to reply to
	}
	resp := nats.NewMsg(msg.Reply)
	resp.Header.Set("Error", errMsg.Error())
	resp.Data = []byte(fmt.Sprintf(`{"error": %q}`, errMsg))
	if err := msg.RespondMsg(resp); err != nil {
		fmt.Printf("Error sending error response: %w, errMsg: %w\n", err, errMsg)
	}
}
