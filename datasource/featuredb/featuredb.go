package featuredb

import (
	"fmt"
	"net"
	"net/http"
	"time"
)

type FeatureDBClient struct {
	Client     *http.Client
	Address    string
	VpcAddress string
	Token      string
}

var (
	featureDBClient *FeatureDBClient
)

func InitFeatureDBClient(address, token, vpcAddress string) {
	if featureDBClient != nil {
		return
	}

	client := &http.Client{
		Transport: &http.Transport{
			MaxConnsPerHost:     1000,
			MaxIdleConns:        1000,
			MaxIdleConnsPerHost: 1000,
			DialContext: (&net.Dialer{
				Timeout: 500 * time.Millisecond,
			}).DialContext,
		},
	}
	featureDBClient = &FeatureDBClient{
		Client:     client,
		Address:    address,
		Token:      token,
		VpcAddress: vpcAddress,
	}
}

func GetFeatureDBClient() (*FeatureDBClient, error) {
	if featureDBClient == nil {
		return nil, fmt.Errorf("FeatureDB has not been provisioned")
	}

	return featureDBClient, nil
}
