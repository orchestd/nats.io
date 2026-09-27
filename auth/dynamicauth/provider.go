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
	conf := auth.NatsSettings{}
	err := config.Get("natsSettings").Unmarshal(&conf)
	if err != nil {
		panic("can't get natsSettings from conf")
	}

	switch conf.AuthType {
	case userpass.AuthType:
		return userpass.NewConfigGetter(credentials, config)
	case jwt.AuthType:
		return jwt.NewConfigGetter(credentials, config)
	default:
		panic(fmt.Sprintf("authType %s is not supported. supports [%s, %s]",
			conf.AuthType, userpass.AuthType, jwt.AuthType))
	}
}
