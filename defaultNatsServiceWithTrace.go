package natsio

import (
	"context"
	"github.com/go-masonry/mortar/utils"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	traceLog "github.com/opentracing/opentracing-go/log"
	"github.com/orchestd/dependencybundler/interfaces/configuration"
	"github.com/orchestd/dependencybundler/interfaces/credentials"
	"github.com/orchestd/dependencybundler/interfaces/log"
	. "github.com/orchestd/servicereply"
	"go.uber.org/fx"
	"time"
)

func NewTraceNatsServiceWithBasicAuth(lc fx.Lifecycle, tracer opentracing.Tracer, config configuration.Config, logger log.Logger, credentials credentials.CredentialsGetter) NatsService {
	service := &natsServiceWithTrace{
		tracer:      tracer,
		config:      config,
		NatsService: NewNatsServiceWithBasicAuth(lc, config, logger, credentials),
	}

	return service
}

func NewTraceNatsServiceWithJWTAuth(lc fx.Lifecycle, tracer opentracing.Tracer, config configuration.Config, credentials credentials.CredentialsGetter, logger log.Logger) NatsService {
	service := &natsServiceWithTrace{
		tracer:      tracer,
		config:      config,
		NatsService: NewNatsServiceWithJWTAuth(lc, config, credentials, logger),
	}

	return service
}

type natsServiceWithTrace struct {
	tracer opentracing.Tracer
	config configuration.Config
	NatsService
}

func addBodyToSpan(span opentracing.Span, name string, msg interface{}) {
	bytes, err := utils.MarshalMessageBody(msg)
	if err == nil {
		span.LogFields(traceLog.String(name, string(bytes)))
	} else {
		span.LogKV(name, msg)
	}
}

func (n natsServiceWithTrace) Request(c context.Context, subj string, data interface{}, timeout time.Duration, target interface{}) ServiceReply {
	sp, _ := opentracing.StartSpanFromContextWithTracer(c, n.tracer, "nats/"+subj)
	defer sp.Finish()
	ext.DBStatement.Set(sp, "nats/"+subj)
	serviceName, _ := n.config.GetServiceName()
	ext.Component.Set(sp, serviceName)
	addBodyToSpan(sp, "request", data)

	reply := n.NatsService.Request(c, subj, data, timeout, target)

	if reply != nil {
		addBodyToSpan(sp, "response", reply)
	} else {
		addBodyToSpan(sp, "response", target)
	}

	return reply
}
