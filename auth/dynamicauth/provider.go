package dynamicauth

import (
	"fmt"

	"github.com/orchestd/dependencybundler/interfaces/configuration"
	"github.com/orchestd/dependencybundler/interfaces/credentials"
	"github.com/orchestd/nats.io/auth"
	"github.com/orchestd/nats.io/auth/jwt"
	"github.com/orchestd/nats.io/auth/userpass"
)

func NewConfigGetter(credentials credentials.CredentialsGetter, config configuration.Config) auth.ConfigGetter {
	settings := auth.NatsSettings{}
	err := config.Get("natsSettings").Unmarshal(&settings)
	if err != nil {
		panic("can't get natsSettings from conf")
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
