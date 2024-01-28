package natsio

import (
	"github.com/nats-io/nats.go"
	. "github.com/orchestd/servicereply"
	"time"
)

type NatsService interface {
	PublishExternal(subj string, msg []byte) error
	Publish(subj string, data interface{}) error
	RequestExternal(subj string, msg []byte, timeout time.Duration) ([]byte, error)
	Request(subj string, data interface{}, timeout time.Duration, target interface{}) ServiceReply
	QueueSubscribe(subj, queue string, handler NatsHandler) error
	QueueSubscribeExternal(subj, queue string, handler NatsHandlerPlainData) error
	Subscribe(subj string, handler NatsHandler) error
	SubscribeExternal(subj string, handler NatsHandlerPlainData) error
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
