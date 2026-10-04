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
	settings    auth.NatsSettings
}

func NewConfigGetter(credentials credentials.CredentialsGetter, config configuration.Config, settings auth.NatsSettings) auth.ConfigGetter {
	return &configGetter{credentials: credentials, settings: settings}
}

func (r *configGetter) GetBackendOption() nats.Option {
	creds, err := r.credentials.GetCredentials().GetNatsCredentials(auth.DefaultBeClientId)
	if err != nil {
		panic(fmt.Sprintf("nats credentials not found for NatsClients[%s], err: %w", auth.DefaultBeClientId, err))
	}
	natsUser := creds.Username
	if natsUser == "" {
		panic("can't get credentials for NatsClients[" + auth.DefaultBeClientId + "].username")
	}
	natsPw := creds.Password
	if natsPw == "" {
		panic("can't get credentials for NatsClients[" + auth.DefaultBeClientId + "].password")
	}
	authOpt := nats.UserInfo(natsUser, natsPw)
	return authOpt
}

func (r *configGetter) GetConfig(ctx context.Context, clientId string) (auth.ConnectionConfig, error) {
	creds, err := r.credentials.GetCredentials().GetNatsCredentials(clientId)
	if err != nil {
		return auth.ConnectionConfig{}, fmt.Errorf("nats credentials not found for NatsClients[%s], err: %w", clientId, err)
	}

	var urls []string
	if creds.Type == "fe" {
		urls = r.settings.FeUrls
	} else {
		urls = r.settings.BeUrls
	}

	if len(urls) == 0 {
		return auth.ConnectionConfig{}, nil
	}

	natsUser := creds.Username
	if natsUser == "" {
		return auth.ConnectionConfig{}, fmt.Errorf("can't get credentials for NatsClients[%s].username", clientId)
	}

	natsPw := creds.Password
	if natsPw == "" {
		return auth.ConnectionConfig{}, fmt.Errorf("can't get credentials for NatsClients[%s].password", clientId)
	}
	return auth.ConnectionConfig{
		AuthType: AuthType,
		Servers:  urls,
		Credentials: Credentials{
			Username: natsUser,
			Password: natsPw,
		},
	}, nil
}

type Credentials struct {
	Username string `json:"username"`
	Password string `json:"password"`
}
