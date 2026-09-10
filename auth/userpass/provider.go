package userpass

import (
	"context"

	"github.com/nats-io/nats.go"
	"github.com/orchestd/dependencybundler/interfaces/configuration"
	"github.com/orchestd/dependencybundler/interfaces/credentials"
	"github.com/orchestd/nats.io/auth"
)

type provider struct {
	credentials credentials.CredentialsGetter
	config      configuration.Config
}

func NewProvider(credentials credentials.CredentialsGetter, config configuration.Config) auth.Provider {
	return &provider{credentials: credentials, config: config}
}

func (r *provider) GetBackendOption() nats.Option {
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

func (r *provider) GetFrontendConfig(ctx context.Context) (auth.FrontendConnectionConfig, error) {
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
	return auth.FrontendConnectionConfig{
		AuthType: "userpass",
		Servers:  []string{wsUrl},
		Credentials: auth.BasicAuthCredentials{
			Username: natsUser,
			Password: natsPw,
		},
	}, nil
}
