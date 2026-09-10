package jwt

import (
	"context"
	"fmt"

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
	natsJwt := r.credentials.GetCredentials().NatsJWT
	if natsJwt == "" {
		panic("can't get credentials by key NatsJWT")
	}
	natsSeed := r.credentials.GetCredentials().NatsSeed
	if natsSeed == "" {
		panic("can't get credentials by key NatsSeed")
	}
	authOpt := nats.UserJWTAndSeed(natsJwt, natsSeed)
	return authOpt
}

func (r *provider) GetFrontendConfig(ctx context.Context) (auth.FrontendConnectionConfig, error) {
	wsUrl, err := r.config.Get("websocketUrl").String()
	if err != nil {
		panic("can't get credentials by key feNatsUser")
	}

	jwt, err := r.config.Get("feNatsJWT").String()
	if err != nil {
		return auth.FrontendConnectionConfig{}, fmt.Errorf("can't get credentials by key feNatsUser")
	}

	return auth.FrontendConnectionConfig{
		AuthType: "jwt",
		Servers:  []string{wsUrl},
		Credentials: auth.JWTAuthCredentials{
			JWT: jwt,
		},
	}, nil
}
