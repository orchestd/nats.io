package natsio

import (
	"bytes"
	"context"
	"github.com/nats-io/nats.go"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/orchestd/dependencybundler/interfaces/configuration"
	"github.com/orchestd/dependencybundler/interfaces/log"
	. "github.com/orchestd/servicereply"
	"go.uber.org/fx"
	"time"
)

func NewDefaultNatsServiceWithTrace(lc fx.Lifecycle, tracer opentracing.Tracer, config configuration.Config, logger log.Logger) NatsService {
	connect = func(natsUrl string, options ...nats.Option) (NatsConnection, error) {
		con, err := nats.Connect(natsUrl, options...)
		if err != nil {
			return nil, err
		}
		return natsServiceWithTrace{
			tracer: tracer,
			logger: logger,
			nc:     con,
		}, nil
	}

	return NewNatsServiceWithJWTAuth(lc, config, logger)
}

type natsServiceWithTrace struct {
	tracer opentracing.Tracer
	logger log.Logger
	nc     NatsConnection
}

func (n natsServiceWithTrace) Close() {
	n.nc.Close()
}

func (n natsServiceWithTrace) Publish(subj string, msg []byte) error {
	var t traceMsg
	pubSpan := n.tracer.StartSpan("Published Message", ext.SpanKindProducer)
	ext.MessageBusDestination.Set(pubSpan, subj)
	defer pubSpan.Finish()

	if err := n.tracer.Inject(pubSpan.Context(), opentracing.Binary, &t); err != nil {
		n.logger.Error(context.Background(), "can't inject trace token for publish subj:%v, err:%v", subj, err)
	}

	t.Write(msg)

	return n.nc.Publish(subj, t.Bytes())
}

func (n natsServiceWithTrace) Request(subj string, msg []byte, timeout time.Duration) (*nats.Msg, error) {
	reqSpan := n.tracer.StartSpan("Service Request", ext.SpanKindRPCClient)
	ext.MessageBusDestination.Set(reqSpan, subj)
	defer reqSpan.Finish()

	var t traceMsg

	if err := n.tracer.Inject(reqSpan.Context(), opentracing.Binary, &t); err != nil {
		n.logger.Error(context.Background(), "can't inject trace token for request subj:%v, err:%v", subj, err)
	}

	t.Write(msg)

	return n.nc.Request(subj, t.Bytes(), timeout)
}

func (n natsServiceWithTrace) QueueSubscribe(subj string, queue string, handler nats.MsgHandler) (*nats.Subscription, error) {
	return n.nc.QueueSubscribe(subj, queue, func(msg *nats.Msg) {
		t := newTraceMsg(msg)

		tokenExtracted := true

		sc, err := n.tracer.Extract(opentracing.Binary, t)
		if err != nil {
			tokenExtracted = false
			n.logger.Error(context.Background(), "can't extract trace token from request subj:%v, err:%v", subj, err)
		}

		if tokenExtracted {
			span := n.tracer.StartSpan("Received Message", ext.SpanKindConsumer, opentracing.FollowsFrom(sc))
			ext.MessageBusDestination.Set(span, msg.Subject)
			defer span.Finish()
		}

		handler(msg)
	})
}

type traceMsg struct {
	bytes.Buffer
}

func newTraceMsg(m *nats.Msg) *traceMsg {
	b := bytes.NewBuffer(m.Data)
	return &traceMsg{*b}
}
