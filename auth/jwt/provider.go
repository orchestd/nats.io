package jwt

import (
	"context"
	"encoding/json"
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
	if r.settings.Frontend.Url == "" {
		return auth.FrontendConnectionConfig{}, nil
	}

	var creds Credentials
	credsBytes, err := json.Marshal(r.settings.Frontend.Creds)
	if err != nil {
		return auth.FrontendConnectionConfig{}, fmt.Errorf("config: natsSettings.frontend.creds failed to marshal: %w", err)
	}

	err = json.Unmarshal(credsBytes, &creds)
	if err != nil {
		return auth.FrontendConnectionConfig{}, fmt.Errorf("config: natsSettings.frontend.creds failed to unmarshal: %w", err)
	}

	jwt := creds.JWT
	if jwt == "" {
		return auth.FrontendConnectionConfig{}, fmt.Errorf("config: natsSettings.frontend.jwt is empty")
	}

	return auth.FrontendConnectionConfig{
		AuthType: AuthType,
		Servers:  []string{r.settings.Frontend.Url},
		Credentials: Credentials{
			JWT: jwt,
		},
	}, nil
}

type Credentials struct {
	JWT string `json:"jwt"`
}
