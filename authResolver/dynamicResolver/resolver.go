package dynamicResolver

import (
	"fmt"

	"github.com/orchestd/dependencybundler/interfaces/configuration"
	"github.com/orchestd/dependencybundler/interfaces/credentials"
	"github.com/orchestd/nats.io/authResolver"
	"github.com/orchestd/nats.io/authResolver/jwtResolver"
	"github.com/orchestd/nats.io/authResolver/userpassResolver"
)

func NewAuthResolver(credentials credentials.CredentialsGetter, config configuration.Config) authResolver.AuthResolver {
	authType, err := config.Get("natsAuthType").String()
	if err != nil {
		panic("can't get authType by key natsAuthType. supports [userpass, jwt]")
	}

	switch authType {
	case "userpass":
		return userpassResolver.NewAuthResolver(credentials, config)
	case "jwt":
		return jwtResolver.NewAuthResolver(credentials, config)
	default:
		panic(fmt.Sprintf("authType %s is not supported"))
	}
}
