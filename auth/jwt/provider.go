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
	jwt := creds.JWT
	if jwt == "" {
		panic(fmt.Sprintf("nats credentials not found for NatsClients[%s].JWT", auth.DefaultBeClientId))
	}
	seed := creds.Seed
	if seed == "" {
		panic(fmt.Sprintf("nats credentials not found for NatsClients[%s].SEED", auth.DefaultBeClientId))
	}

	authOpt := nats.UserJWTAndSeed(jwt, seed)
	return authOpt
}

func (r *configGetter) GetConfig(ctx context.Context, clientId string) (auth.ConnectionConfig, error) {
	if len(r.settings.FeUrls) == 0 {
		return auth.ConnectionConfig{}, nil
	}

	creds, err := r.credentials.GetCredentials().GetNatsCredentials(clientId)
	if err != nil {
		return auth.ConnectionConfig{}, fmt.Errorf("can't get credentials for NATS_CLIENTS[%s], err: %w", clientId, err)
	}

	jwt := creds.JWT
	if jwt == "" {
		return auth.ConnectionConfig{}, fmt.Errorf("can't get credentials for NATS_CLIENTS[%s].JWT", clientId)
	}

	return auth.ConnectionConfig{
		AuthType: AuthType,
		Servers:  r.settings.FeUrls,
		Credentials: Credentials{
			JWT: jwt,
		},
	}, nil
}

type Credentials struct {
	JWT string `json:"jwt"`
}
