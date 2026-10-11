package helpers

import (
	"fmt"

	credentialtypes "github.com/orchestd/configurations/credentials"
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

func GetUrlsAndCreds(credentials credentialsgetters.CredentialsGetter, settings auth.NatsSettings, clientId string) ([]string, credentialtypes.NatsCredentials, error) {
	var urls []string

	creds, err := credentials.GetCredentials().GetNatsCredentials(clientId)
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
