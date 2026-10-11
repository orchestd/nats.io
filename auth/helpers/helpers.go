package helpers

import (
	"fmt"

	credentialtypes "github.com/orchestd/configurations/credentials"
	"github.com/orchestd/dependencybundler/interfaces/configuration"
	credentialsgetters "github.com/orchestd/dependencybundler/interfaces/credentials"
	"github.com/orchestd/nats.io/auth"
)

func GetErrorMessage(clientId, property string) error {
	msg := fmt.Sprintf("can't get credentials for NatsClients[%s]", clientId)
	if property != "" {
		msg = fmt.Sprintf("%s.%s", msg, property)
	}
	return fmt.Errorf(msg)
}

func GetUrlsAndCreds(credentials credentialsgetters.CredentialsGetter, config configuration.Config, settings auth.NatsSettings, clientId string) ([]string, credentialtypes.NatsCredentials, error) {
	var urls []string

	serviceName, err := config.GetServiceName()
	if err != nil {
		return urls, credentialtypes.NatsCredentials{}, err
	}
	creds, err := credentials.GetCredentials().GetServiceNatsCredentials(serviceName, clientId)
	if err != nil {
		return urls, creds, fmt.Errorf("%w; %w", GetErrorMessage(clientId, ""), err)
	}

	if creds.Type == "fe" {
		urls = settings.FeUrls
	} else {
		urls = settings.BeUrls
	}

	return urls, creds, nil
}
