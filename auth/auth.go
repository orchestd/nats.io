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

type ConfigGetter interface {
	GetBackendOption() nats.Option
	GetFrontendConfig(ctx context.Context) (FrontendConnectionConfig, error)
}

// NatsSettingsI structure is identical to struct NatsSettings
type NatsSettingsI interface{}

type NatsSettings struct {
	AuthType string                    `json:"authType"`
	Url      string                    `json:"url"`
	Frontend NatsFrontendConfiguration `json:"frontend"`
}

type NatsFrontendConfiguration struct {
	Url         string      `json:"url"`
	Credentials interface{} `json:"credentials"`
}
