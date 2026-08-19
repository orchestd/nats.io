package userpassResolver

import (
	"context"

	"github.com/nats-io/nats.go"
	"github.com/orchestd/dependencybundler/interfaces/configuration"
	"github.com/orchestd/dependencybundler/interfaces/credentials"
	"github.com/orchestd/nats.io/authResolver"
)

type basicAuthResolver struct {
	credentials credentials.CredentialsGetter
	config      configuration.Config
}

func NewAuthResolver(credentials credentials.CredentialsGetter, config configuration.Config) authResolver.AuthResolver {
	return &basicAuthResolver{credentials: credentials, config: config}
}

func (r *basicAuthResolver) GetBackendOption() nats.Option {
	natsUser := r.credentials.GetCredentials().NatsUser
	if natsUser == "" {
		panic("can't get credentials by key NatsUser")
	}
	natsPw := r.credentials.GetCredentials().NatsPw
	if natsPw == "" {
		panic("can't get credentials by key NatsPw")
	}
	authOpt := nats.UserInfo(natsUser, natsPw)
	return authOpt
}

func (r *basicAuthResolver) GetFrontendConfig(ctx context.Context) (authResolver.FrontendConnectionConfig, error) {
	wsUrl, err := r.config.Get("websocketUrl").String()
	if err != nil {
		panic("can't get credentials by key feNatsUser")
	}

	natsUser, err := r.config.Get("feNatsUser").String()
	if err != nil {
		panic("can't get credentials by key feNatsUser")
	}

	natsPw, err := r.config.Get("feNatsPw").String()
	if err != nil {
		panic("can't get credentials by key feNatsPw")
	}
	return authResolver.FrontendConnectionConfig{
		AuthType: "userpass",
		Servers:  []string{wsUrl},
		Credentials: authResolver.BasicAuthCredentials{
			Username: natsUser,
			Password: natsPw,
		},
	}, nil
}
