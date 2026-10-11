package userpass

import (
	"context"

	"github.com/nats-io/nats.go"
	"github.com/orchestd/dependencybundler/interfaces/configuration"
	"github.com/orchestd/dependencybundler/interfaces/credentials"
	"github.com/orchestd/nats.io/auth"
	"github.com/orchestd/nats.io/auth/helpers"
)

const AuthType = "userpass"

type configGetter struct {
	credentials credentials.CredentialsGetter
	config      configuration.Config
	settings    auth.NatsSettings
}

func NewConfigGetter(credentials credentials.CredentialsGetter, config configuration.Config, settings auth.NatsSettings) auth.ConfigGetter {
	return &configGetter{credentials: credentials, config: config, settings: settings}
}

func (r *configGetter) GetBackendOption() nats.Option {
	_, creds, err := helpers.GetUrlsAndCreds(r.credentials, r.config, r.settings, auth.DefaultBeClientId)
	if err != nil {
		panic(err)
	}

	username := creds.Username
	if username == "" {
		panic(helpers.GetErrorMessage(auth.DefaultBeClientId, "username"))
	}
	password := creds.Password
	if password == "" {
		panic(helpers.GetErrorMessage(auth.DefaultBeClientId, "password"))
	}
	authOpt := nats.UserInfo(username, password)
	return authOpt
}

func (r *configGetter) GetConfig(ctx context.Context, clientId string) (auth.ConnectionConfig, error) {
	urls, creds, err := helpers.GetUrlsAndCreds(r.credentials, r.config, r.settings, clientId)
	if err != nil {
		return auth.ConnectionConfig{}, err
	}

	if len(urls) == 0 {
		return auth.ConnectionConfig{}, nil
	}

	username := creds.Username
	if username == "" {
		return auth.ConnectionConfig{}, helpers.GetErrorMessage(clientId, "username")
	}

	password := creds.Password
	if password == "" {
		return auth.ConnectionConfig{}, helpers.GetErrorMessage(clientId, "password")
	}
	return auth.ConnectionConfig{
		AuthType: AuthType,
		Servers:  urls,
		Credentials: Credentials{
			Username: username,
			Password: password,
		},
	}, nil
}

type Credentials struct {
	Username string `json:"username"`
	Password string `json:"password"`
}
