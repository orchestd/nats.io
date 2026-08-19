package authResolver

import (
	"context"

	"github.com/nats-io/nats.go"
)

type FrontendConnectionConfig struct {
	AuthType    string      `json:"authType"` // "basic", "jwt", "anonymous"
	Servers     []string    `json:"servers"`
	Credentials interface{} `json:"credentials"`
}

// BasicAuthCredentials payload for frontend clients using basic auth
type BasicAuthCredentials struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// JWTAuthCredentials payload for frontend clients using JWT auth
type JWTAuthCredentials struct {
	JWT string `json:"jwt"`
}

// AuthResolver is the single source of truth for both backend and frontend auth
type AuthResolver interface {
	// GetBackendOption builds the option for the internal Go NATS client
	GetBackendOption() nats.Option
	// GetFrontendConfig prepares the safe handshake payload for frontend clients
	GetFrontendConfig(ctx context.Context) (FrontendConnectionConfig, error)
}
