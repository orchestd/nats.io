package dynamicauth

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/orchestd/dependencybundler/interfaces/configuration"
	"github.com/orchestd/dependencybundler/interfaces/credentials"
	"github.com/orchestd/nats.io/auth"
	"github.com/orchestd/nats.io/auth/jwt"
	"github.com/orchestd/nats.io/auth/userpass"
)

func NewConfigGetter(credentials credentials.CredentialsGetter, config configuration.Config) auth.ConfigGetter {
	settings := auth.NatsSettings{}
	envsettings := os.Getenv("natsSettings")
	if envsettings == "" {
		panic("can't get natsSettings from env")
	}

	err := json.Unmarshal([]byte(envsettings), &settings)
	if err != nil {
		panic("can't unmarshal natsSettings " + err.Error())
	}

	switch settings.AuthType {
	case userpass.AuthType:
		return userpass.NewConfigGetter(credentials, config, settings)
	case jwt.AuthType:
		return jwt.NewConfigGetter(credentials, config, settings)
	default:
		panic(fmt.Sprintf("authType %s is not supported. supports [%s, %s]",
			settings.AuthType, userpass.AuthType, jwt.AuthType))
	}
}
