package auth

import (
	"context"

	"github.com/nats-io/nats.go"
)

const (
	DefaultBeClientId = "be"
	DefaultFeClientId = "fe"
)

type ConnectionConfig struct {
	AuthType    string      `json:"authType"`
	Servers     []string    `json:"servers"`
	Credentials interface{} `json:"credentials"`
}

type ConfigGetter interface {
	GetBackendOption() nats.Option
	GetConfig(ctx context.Context, clientId string) (ConnectionConfig, error)
}

// NatsSettingsI structure is identical to struct NatsSettings
type NatsSettingsI interface{}

type NatsSettings struct {
	AuthType string   `json:"authType"`
	BeUrls   []string `json:"beUrls"`
	FeUrls   []string `json:"feUrls"`
}
