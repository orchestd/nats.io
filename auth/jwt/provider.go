package jwt

import (
	"context"

	"github.com/nats-io/nats.go"
	"github.com/orchestd/dependencybundler/interfaces/configuration"
	"github.com/orchestd/dependencybundler/interfaces/credentials"
	"github.com/orchestd/nats.io/auth"
	"github.com/orchestd/nats.io/auth/helpers"
)

const AuthType = "jwt"

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

	jwt := creds.JWT
	if jwt == "" {
		panic(helpers.GetErrorMessage(auth.DefaultBeClientId, "jwt"))
	}
	seed := creds.Seed
	if seed == "" {
		panic(helpers.GetErrorMessage(auth.DefaultBeClientId, "seed"))
	}

	authOpt := nats.UserJWTAndSeed(jwt, seed)
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

	jwt := creds.JWT
	if jwt == "" {
		return auth.ConnectionConfig{}, helpers.GetErrorMessage(clientId, "jwt")
	}

	return auth.ConnectionConfig{
		AuthType: AuthType,
		Servers:  urls,
		Credentials: Credentials{
			JWT: jwt,
		},
	}, nil
}

type Credentials struct {
	JWT string `json:"jwt"`
}
