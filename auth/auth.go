package auth

import (
	"context"

	"github.com/nats-io/nats.go"
)

type FrontendConnectionConfig struct {
	AuthType    string      `json:"authType"`
	Servers     []string    `json:"servers"`
	Credentials interface{} `json:"credentials"`
}

type BasicAuthCredentials struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type JWTAuthCredentials struct {
	JWT string `json:"jwt"`
}

type Provider interface {
	GetBackendOption() nats.Option
	GetFrontendConfig(ctx context.Context) (FrontendConnectionConfig, error)
}
