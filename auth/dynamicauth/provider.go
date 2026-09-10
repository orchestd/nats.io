package dynamicauth

import (
	"fmt"

	"github.com/orchestd/dependencybundler/interfaces/configuration"
	"github.com/orchestd/dependencybundler/interfaces/credentials"
	"github.com/orchestd/nats.io/auth"
	"github.com/orchestd/nats.io/auth/jwt"
	"github.com/orchestd/nats.io/auth/userpass"
)

func NewProvider(credentials credentials.CredentialsGetter, config configuration.Config) auth.Provider {
	authType, err := config.Get("natsAuthType").String()
	if err != nil {
		panic("can't get authType by key natsAuthType. supports [userpass, jwt]")
	}

	switch authType {
	case "userpass":
		return userpass.NewProvider(credentials, config)
	case "jwt":
		return jwt.NewProvider(credentials, config)
	default:
		panic(fmt.Sprintf("authType %s is not supported"))
	}
}
