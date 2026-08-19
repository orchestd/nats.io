package natsio

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/orchestd/nats.io/middlewares"
	. "github.com/orchestd/servicereply"
)

type NatsService interface {
	PublishExternal(subj string, msg []byte) error
	Publish(subj string, data interface{}) error
	RequestExternal(subj string, msg []byte, timeout time.Duration) ([]byte, error)
	Request(c context.Context, subj string, data interface{}, timeout time.Duration, target interface{}) ServiceReply
	QueueSubscribe(subj, queue string, handler NatsHandler, middlewares ...middlewares.Middleware) error
	QueueSubscribeExternal(subj, queue string, handler NatsHandlerPlainData, middlewares ...middlewares.Middleware) error
	Subscribe(subj string, handler NatsHandler, middlewares ...middlewares.Middleware) error
	SubscribeExternal(subj string, handler NatsHandlerPlainData, middlewares ...middlewares.Middleware) error
	Unsubscribe(subj string) error
	QueueUnsubscribe(subj, queue string) error
	SetConnectionFailedHandler(func(err error))
	Connect(natsUrl, serviceName string, authOpt nats.Option,
		connectionAttempts, reconnectionAttempts, reconnectWaitSec, maxPingsOutstanding, pingIntervalSec int) error
	IsNoResponderErr(err error) bool
}

type NatsConnection interface {
	Close()
	Publish(subj string, msg []byte) error
	Request(subj string, msg []byte, timeout time.Duration) (*nats.Msg, error)
	QueueSubscribe(subj string, queue string, handler nats.MsgHandler) (*nats.Subscription, error)
}

type NatsHandler interface {
	Exec() (interface{}, ServiceReply)
}

type NatsHandlerPlainData interface {
	Exec(data []byte) []byte
}

type NatsServiceConfiguration struct {
	NatsAuthType string `json:"natsAuthType"`
	NatsUrl      string `json:"natsUrl"`
	WebsocketUrl string `json:"websocketUrl"`
	FeNatsJWT    string `json:"feNatsJWT"`
	FeNatsPw     string `json:"feNatsPw"`
	FeNatsUser   string `json:"feNatsUser"`
}
