package userpass

import (
	"context"

	"github.com/nats-io/nats.go"
	credentialtypes "github.com/orchestd/configurations/credentials"
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

	err = validateUserPassword(auth.DefaultBeClientId, creds)
	if err != nil {
		panic(err)
	}

	authOpt := nats.UserInfo(creds.Username, creds.Password)
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

	err = validateUserPassword(clientId, creds)
	if err != nil {
		return auth.ConnectionConfig{}, err
	}

	return auth.ConnectionConfig{
		AuthType: AuthType,
		Servers:  urls,
		Credentials: Credentials{
			Username: creds.Username,
			Password: creds.Password,
		},
	}, nil
}

type Credentials struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

func validateUserPassword(clientId string, creds credentialtypes.NatsCredentials) error {
	if creds.Username == "" {
		return helpers.GetErrorMessage(clientId, "username")
	}
	if creds.Password == "" {
		return helpers.GetErrorMessage(clientId, "password")
	}

	return nil
}
