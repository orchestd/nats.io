package jwt

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/orchestd/dependencybundler/interfaces/configuration"
	"github.com/orchestd/dependencybundler/interfaces/credentials"
	"github.com/orchestd/nats.io/auth"
)

const AuthType = "jwt"

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

func (r *configGetter) GetFrontendConfig(ctx context.Context) (auth.FrontendConnectionConfig, error) {
	if r.config.Frontend.Url == "" {
		return auth.FrontendConnectionConfig{}, nil
	}

	jwt := r.config.Frontend.JWT
	if jwt == "" {
		return auth.FrontendConnectionConfig{}, fmt.Errorf("config: natsSettings.frontend.jwt is empty")
	}

	return auth.FrontendConnectionConfig{
		AuthType: AuthType,
		Servers:  []string{r.config.Frontend.Url},
		Credentials: auth.JWTAuthCredentials{
			JWT: jwt,
		},
	}, nil
}
