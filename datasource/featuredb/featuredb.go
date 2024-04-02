package featuredb

import (
	"fmt"
	"net/http"
)

type FeatureDBClient struct {
	Client  *http.Client
	Address string
	Token   string
}

var (
	featureDBClient *FeatureDBClient
)

func InitFeatureDBClient(address, token string) {
	if featureDBClient != nil {
		return
	}

	client := &http.Client{
		Transport: &http.Transport{
			MaxConnsPerHost:     1000,
			MaxIdleConns:        1000,
			MaxIdleConnsPerHost: 1000,
		},
	}
	featureDBClient = &FeatureDBClient{
		Client:  client,
		Address: address,
		Token:   token,
	}
}

func GetFeatureDBClient() (*FeatureDBClient, error) {
	if featureDBClient == nil {
		return nil, fmt.Errorf("FeatureDB has not been provisioned")
	}

	return featureDBClient, nil
}
