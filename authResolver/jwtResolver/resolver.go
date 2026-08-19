package jwtResolver

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/orchestd/dependencybundler/interfaces/configuration"
	"github.com/orchestd/dependencybundler/interfaces/credentials"
	"github.com/orchestd/nats.io/authResolver"
)

type jwtAuthResolver struct {
	credentials credentials.CredentialsGetter
	config      configuration.Config
}

func NewAuthResolver(credentials credentials.CredentialsGetter, config configuration.Config) authResolver.AuthResolver {
	return &jwtAuthResolver{credentials: credentials, config: config}
}

func (r *jwtAuthResolver) GetBackendOption() nats.Option {
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

func (r *jwtAuthResolver) GetFrontendConfig(ctx context.Context) (authResolver.FrontendConnectionConfig, error) {
	natsUser, err := r.config.Get("feNatsJWT").String()
	if err != nil {
		return authResolver.FrontendConnectionConfig{}, fmt.Errorf("can't get credentials by key feNatsUser")
	}

	return authResolver.FrontendConnectionConfig{
		AuthType: "basic",
		Servers:  []string{},
		Credentials: authResolver.BasicAuthCredentials{
			Username: natsUser,
		},
	}, nil
}
