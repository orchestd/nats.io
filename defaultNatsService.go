package natsio

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/orchestd/dependencybundler/interfaces/configuration"
	"github.com/orchestd/dependencybundler/interfaces/credentials"
	"github.com/orchestd/dependencybundler/interfaces/log"
	. "github.com/orchestd/servicereply"
	"github.com/orchestd/servicereply/status"
	"go.uber.org/fx"
	"reflect"
	"time"
)

var connect = func(natsUrl string, options ...nats.Option) (NatsConnection, error) {
	return nats.Connect(natsUrl, options...)
}

func NewDefaultNatsService(lc fx.Lifecycle, config configuration.Config, logger log.Logger, credentials credentials.CredentialsGetter) NatsService {
	return getDefaultService(lc, config, logger, func(serviceName string) nats.Option {
		natsUser := credentials.GetCredentials().NatsUser
		if natsUser == "" {
			panic("NotificationService could not get credentials by key NatsUser")
		}
		natsPw := credentials.GetCredentials().NatsPw
		if natsPw == "" {
			panic("NotificationService could not get credentials by key NatsPw")
		}
		authOpt := nats.UserInfo(natsUser, natsPw)
		return authOpt
	})
}

func NewDefaultNatsServiceWithJWTAuth(lc fx.Lifecycle, config configuration.Config, logger log.Logger) NatsService {
	return getDefaultService(lc, config, logger, func(serviceName string) nats.Option {
		authOpt := nats.UserCredentials(serviceName + ".creds")
		return authOpt
	})
}

func getDefaultService(lc fx.Lifecycle, config configuration.Config, logger log.Logger, getAuthOpts func(serviceName string) nats.Option) NatsService {
	service := defaultNatsService{
		logger:        logger,
		subscriptions: map[string]*nats.Subscription{},
	}
	serviceName, err := config.GetServiceName()
	if err != nil {
		panic("can't get serviceName: " + err.Error())
	}
	natsUrl, err := config.Get("NatsUrl").String()
	if err != nil {
		panic("could not get conf by key NatsUrl " + err.Error())
	}
	connectionAttempts, err := config.Get("connectionAttempts").Int()
	if err != nil {
		connectionAttempts = 1
	}
	reconnectionAttempts, err := config.Get("reconnectionAttempts").Int()
	if err != nil {
		reconnectionAttempts = -1
	}
	authOpt := getAuthOpts(serviceName)
	lc.Append(fx.Hook{
		OnStart: func(c context.Context) error {
			return service.Connect(natsUrl, serviceName, authOpt, connectionAttempts, reconnectionAttempts)
		},
		OnStop: func(c context.Context) error {
			service.Close()
			return nil
		},
	})
	return &service
}

type defaultNatsService struct {
	nc            NatsConnection
	logger        log.Logger
	subscriptions map[string]*nats.Subscription
}

func (n defaultNatsService) Connect(natsUrl, serviceName string, authOpt nats.Option, connectionAttempts, reconnectionAttempts int) error {
	opts := []nats.Option{nats.Name(serviceName)}
	opts = append(opts, authOpt)
	opts = append(opts, nats.ReconnectWait(2*time.Second))
	opts = append(opts, nats.MaxReconnects(reconnectionAttempts))
	opts = append(opts, nats.MaxPingsOutstanding(2))
	opts = append(opts, nats.PingInterval(2*time.Second))

	opts = append(opts, nats.ErrorHandler(func(_ *nats.Conn, s *nats.Subscription, err error) {
		n.logger.Error(context.Background(), "nats.ErrorHandler fired, Subscription: %v, err: %v", s, err)
	}))

	opts = append(opts, nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
		n.logger.Error(context.Background(), "nats.DisconnectErrHandler fired, err: %v", err)
	}))

	opts = append(opts, nats.ReconnectHandler(func(nc *nats.Conn) {
		n.logger.Warn(context.Background(), fmt.Sprintf("nats.ReconnectHandler [%s]", nc.ConnectedUrl()))
	}))

	err := n.connect(natsUrl, opts)
	if err != nil {
		n.logger.Error(context.Background(), "can't connect to nats server %s err: %v", natsUrl, err)
		if connectionAttempts < 2 {
			return err
		}
		go func() {
			for i := 1; i <= connectionAttempts; i++ {
				time.Sleep(time.Minute)
				err := n.connect(natsUrl, opts)
				if err == nil {
					break
				}
				n.logger.Error(context.Background(), "can't connect to nats server %s err: %v. attempt - %v", natsUrl, err, i)
			}
			n.logger.Error(context.Background(), "can't connect to nats server %s after %v attempts. Stop trying", natsUrl, connectionAttempts)
		}()
		return nil
	}
	return nil
}

func (n *defaultNatsService) connect(natsUrl string, opts []nats.Option) error {
	nc, err := connect(natsUrl, opts...)
	if err != nil {
		return err
	}
	n.nc = nc
	return nil
}

func (n defaultNatsService) Close() {
	if n.checkIsReady() == nil {
		n.nc.Close()
	}
}

func (n defaultNatsService) checkIsReady() error {
	if n.nc == nil {
		return fmt.Errorf("nats server is not ready")
	}
	return nil
}

func (n defaultNatsService) PublishExternal(subj string, msg []byte) error {
	err := n.checkIsReady()
	if err != nil {
		return err
	}
	err = n.nc.Publish(subj, msg)
	if err != nil {
		return fmt.Errorf("can't publish message subject: %v. error: %v", subj, err)
	}
	return nil
}

func (n defaultNatsService) Publish(subj string, data interface{}) error {
	b, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("PublishJSON data can't be convert into JSON. %s", err)
	}
	return n.PublishExternal(subj, b)
}

func (n defaultNatsService) RequestExternal(subj string, msg []byte, timeout time.Duration) ([]byte, error) {
	err := n.checkIsReady()
	if err != nil {
		return nil, err
	}
	resp, err := n.nc.Request(subj, msg, timeout)
	if err != nil {
		return nil, fmt.Errorf("can't Request message subject: %v. error: %v", subj, err)
	}
	return resp.Data, nil
}

func (n defaultNatsService) Request(subj string, data interface{}, timeout time.Duration, target interface{}) ServiceReply {
	b, err := json.Marshal(data)
	if err != nil {
		return NewInternalServiceError(fmt.Errorf("RequestJSON data can't be convert into JSON. %s", err))
	}
	rb, err := n.RequestExternal(subj, b, timeout)
	if err != nil {
		return NewInternalServiceError(err)
	}
	err = handleInternalResponse(rb, target)
	if err != nil {
		return NewInternalServiceError(fmt.Errorf("can't handle Internal Response: " + err.Error()))
	}
	return nil
}

func (n defaultNatsService) QueueSubscribe(subj, queue string, handler NatsHandler) error {
	if n.nc == nil {
		return fmt.Errorf("can't subscribe. no connection to nats server")
	}
	if n.checkSubscriptionExist(subj, queue) {
		return fmt.Errorf("subscription for subj:%v and queue:%v already exists", subj, queue)
	}
	subscription, err := n.nc.QueueSubscribe(subj, queue, func(msg *nats.Msg) {
		newHandler := createNewInnerHandler(handler)
		err := json.Unmarshal(msg.Data, &newHandler)
		if err != nil {
			b := errorReply(NewBadRequestError("can't unmarshal request error: "+err.Error()), nil)
			n.respond(subj, queue, msg, b)
			return
		}
		resp, sErr := newHandler.Exec()
		if sErr != nil {
			b := errorReply(sErr, resp)
			n.respond(subj, queue, msg, b)
			return
		}

		b := successReply(resp)
		if err != nil {
			b := errorReply(NewInternalServiceError(fmt.Errorf("can't marshal resp error: "+err.Error())), nil)
			n.respond(subj, queue, msg, b)
			return
		}
		n.respond(subj, queue, msg, b)
	})
	if err != nil {
		return fmt.Errorf("can't queue subscribe subj: %v, queue: %v. error: %v", subj, queue, err)
	}
	n.addSubscription(subj, queue, subscription)
	return nil
}

func (n *defaultNatsService) QueueSubscribeExternal(subj, queue string, handler NatsHandlerPlainData) error {
	if n.nc == nil {
		return fmt.Errorf("can't subscribe. no connection to nats server")
	}
	if n.checkSubscriptionExist(subj, queue) {
		return fmt.Errorf("subscription for subj:%v and queue:%v already exists", subj, queue)
	}
	subscription, err := n.nc.QueueSubscribe(subj, queue, func(msg *nats.Msg) {
		resp := handler.Exec(msg.Data)
		err := msg.Respond(resp)
		n.logger.Error(context.Background(), "can't send response on queue subscribe subj: %v, queue: %v. error: %v", subj, queue, err)
	})
	if err != nil {
		return fmt.Errorf("can't queue subscribe subj: %v, queue: %v. error: %v", subj, queue, err)
	}
	n.addSubscription(subj, queue, subscription)
	return nil
}

func (n defaultNatsService) Subscribe(subj string, handler NatsHandler) error {
	return n.QueueSubscribe(subj, "", handler)
}

func (n *defaultNatsService) SubscribeExternal(subj string, handler NatsHandlerPlainData) error {
	return n.QueueSubscribeExternal(subj, "", handler)
}

func (n defaultNatsService) respond(subj, queue string, msg *nats.Msg, b []byte) {
	err := n.nc.Publish(msg.Reply, b)
	if err != nil {
		n.logger.Error(context.Background(), "can't send response on queue subscribe subj: %v, queue: %v. error: %v", subj, queue, err)
	}
}

func (n *defaultNatsService) checkSubscriptionExist(subj, queue string) bool {
	_, ok := n.subscriptions[subj+queue]
	return ok
}

func (n *defaultNatsService) addSubscription(subj, queue string, subscription *nats.Subscription) {
	n.subscriptions[subj+queue] = subscription
}

func handleInternalResponse(body []byte, target interface{}) (srvReply ServiceReply) {
	var srvError Response
	srvReply = NewNil()
	if err := json.Unmarshal(body, &srvError); err != nil {
		return NewInternalServiceError(err).WithLogMessage(fmt.Sprintf("cannot read response from")).WithLogValues(ValuesMap{"rawResponse": string(body)})
	}
	if srvError.Status != status.SuccessStatus {
		resType := status.GetTypeByStatus(srvError.GetStatus())
		msgValues := srvError.GetMessageValues()
		srvReply = NewServiceError(&resType, fmt.Errorf(string(srvError.GetStatus())+". "+srvError.GetMessageId()), srvError.GetMessageId(), 1)
		if msgValues != nil {
			srvReply = srvReply.WithReplyValues(*msgValues)
		}
		return srvReply
	}
	if srvError.Message != nil {
		msgValues := srvError.GetMessageValues()
		srvReply = NewMessage(srvError.GetMessageId())
		if msgValues != nil {
			srvReply = srvReply.WithReplyValues(*msgValues)
		}
	}
	if srvError.Data != nil {
		if dataJson, err := json.Marshal(srvError.Data); err != nil {
			return NewInternalServiceError(fmt.Errorf("can't unmarshal serviceReply response. " + err.Error()))
		} else {
			body = dataJson
		}
	} else {
		body = nil
	}
	if err := json.Unmarshal(body, &target); err != nil {
		return NewInternalServiceError(fmt.Errorf("can't unmarshal response data. " + err.Error()))
	}
	return nil
}

func createNewInnerHandler(innerHandler NatsHandler) NatsHandler {
	v := reflect.ValueOf(innerHandler)

	if v.Type().Kind() == reflect.Ptr {
		v = v.Elem()
	}
	n := reflect.New(v.Type())
	return n.Interface().(NatsHandler)

}

func errorReply(err ServiceReply, res interface{}) []byte {
	serviceReply := Response{}
	serviceReply.Status = status.GetStatus(err.GetErrorType())

	serviceReply.Message = &Message{
		Id:     err.GetUserError(),
		Values: err.GetReplyValues(),
	}

	if err.IsSuccess() && res != nil {
		serviceReply.Data = res
	}
	r, _ := json.Marshal(serviceReply)
	return r
}

func successReply(reply interface{}) []byte {
	serviceReply := Response{}
	serviceReply.Status = status.SuccessStatus
	serviceReply.Data = reply
	r, _ := json.Marshal(serviceReply)
	return r
}
