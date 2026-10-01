package userpass

import (
	"context"
	"encoding/json"
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
	if r.settings.Frontend.Url == "" {
		return auth.FrontendConnectionConfig{}, nil
	}

	var creds Credentials
	credsBytes, err := json.Marshal(r.settings.Frontend.Credentials)
	if err != nil {
		return auth.FrontendConnectionConfig{}, fmt.Errorf("config: natsSettings.frontend.creds failed to marshal: %w", err)
	}

	err = json.Unmarshal(credsBytes, &creds)
	if err != nil {
		return auth.FrontendConnectionConfig{}, fmt.Errorf("config: natsSettings.frontend.creds failed to unmarshal: %w", err)
	}

	natsUser := creds.Username
	if natsUser == "" {
		return auth.FrontendConnectionConfig{}, fmt.Errorf("config: natsSettings.frontend.creds.username is empty")
	}

	natsPw := creds.Password
	if natsPw == "" {
		return auth.FrontendConnectionConfig{}, fmt.Errorf("config: natsSettings.frontend.creds.password is empty")
	}
	return auth.FrontendConnectionConfig{
		AuthType: AuthType,
		Servers:  []string{r.settings.Frontend.Url},
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
