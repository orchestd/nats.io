package userpass

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/orchestd/dependencybundler/interfaces/configuration"
	"github.com/orchestd/dependencybundler/interfaces/credentials"
	"github.com/orchestd/nats.io/auth"
)

const AuthType = "userpass"

type configGetter struct {
	credentials credentials.CredentialsGetter
	config      auth.NatsSettings
}

func NewConfigGetter(credentials credentials.CredentialsGetter, config configuration.Config) auth.ConfigGetter {
	conf := auth.NatsSettings{}
	err := config.Get("natsSettings").Unmarshal(&conf)
	if err != nil {
		panic("can't get natsSettings from conf")
	}
	return &configGetter{credentials: credentials, config: conf}
}

func (r *configGetter) GetBackendOption() nats.Option {
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

func (r *configGetter) GetFrontendConfig(ctx context.Context) (auth.FrontendConnectionConfig, error) {
	if r.config.Frontend.Url == "" {
		return auth.FrontendConnectionConfig{}, nil
	}

	natsUser := r.config.Frontend.User
	if natsUser == "" {
		return auth.FrontendConnectionConfig{}, fmt.Errorf("config: natsSettings.frontend.user is empty")
	}

	natsPw := r.config.Frontend.Password
	if natsPw == "" {
		return auth.FrontendConnectionConfig{}, fmt.Errorf("config: natsSettings.frontend.password is empty")
	}
	return auth.FrontendConnectionConfig{
		AuthType: AuthType,
		Servers:  []string{r.config.Frontend.Url},
		Credentials: auth.BasicAuthCredentials{
			Username: natsUser,
			Password: natsPw,
		},
	}, nil
}
