package middlewares

import (
	"github.com/nats-io/nats.go"
)

type Middleware interface {
	Exec(next func(msg *nats.Msg)) func(msg *nats.Msg)
}
