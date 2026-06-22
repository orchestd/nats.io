package handshake

import (
	"encoding/json"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/orchestd/nats.io/middlewares"
)

var (
	MissingHeaderErr = fmt.Errorf("Handshake-mw: Missing Handshake header\n")
	HeaderParseErr   = fmt.Errorf("Handshake-mw: error parsing handshake\n")
	InvalidErr       = fmt.Errorf("Handshake-mw: handshake invalid\n")
)

type handshake struct {
	factory Factory
}

type Handshake interface {
	Validate() error
}

type Factory func() Handshake

func NewMiddleware(factory Factory) middlewares.Middleware {
	return &handshake{factory: factory}
}

func (mw handshake) Exec(next func(msg *nats.Msg)) func(msg *nats.Msg) {
	return func(msg *nats.Msg) {
		hsStr := msg.Header.Get("Handshake")
		if hsStr == "" {
			replyError(msg, MissingHeaderErr)
			return
		}

		hs := mw.factory()
		err := json.Unmarshal([]byte(hsStr), &hs)
		if err != nil {
			replyError(msg, fmt.Errorf("%wErr: %s", HeaderParseErr, err.Error()))
			return
		}

		if hs.Validate() != nil {
			replyError(msg, InvalidErr)
		}

		next(msg)
	}
}

func replyError(msg *nats.Msg, errMsg error) {
	if msg.Reply == "" {
		return // fire-and-forget message, nothing to reply to
	}
	resp := nats.NewMsg(msg.Reply)
	errData := struct {
		Error string `json:"error"`
	}{
		Error: errMsg.Error(),
	}

	bytes, err := json.Marshal(errData)
	if err != nil {
		fmt.Printf("Error marshalling error-response: %+v, err: %w\n", errData, err)
		return
	}

	resp.Data = bytes
	if err := msg.RespondMsg(resp); err != nil {
		fmt.Printf("Error sending error response: %w, errMsg: %w\n", err, errMsg)
	}
}
