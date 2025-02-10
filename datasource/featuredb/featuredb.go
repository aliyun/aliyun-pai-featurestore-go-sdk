package featuredb

import (
	"fmt"
	"net"
	"net/http"
	"sync/atomic"
	"time"
)

type FeatureDBClient struct {
	Client     *http.Client
	address    string
	Token      string
	vpcAddress string

	useVpcAddress atomic.Bool
	checkInterval time.Duration
	stopChan      chan struct{}
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
				Timeout:   200 * time.Millisecond,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			ResponseHeaderTimeout: 500 * time.Millisecond,
		},
	}
	featureDBClient = &FeatureDBClient{
		Client:        client,
		address:       address,
		Token:         token,
		vpcAddress:    fmt.Sprintf("http://%s", vpcAddress),
		checkInterval: 1 * time.Minute,
		stopChan:      make(chan struct{}),
	}

	featureDBClient.useVpcAddress.Store(false)

	if vpcAddress != "" {
		featureDBClient.CheckVpcAddress()

		go featureDBClient.backgroundCheckVpcAddress()
	}
}

func GetFeatureDBClient() (*FeatureDBClient, error) {
	if featureDBClient == nil {
		return nil, fmt.Errorf("FeatureDB has not been provisioned")
	}

	return featureDBClient, nil
}

func (f *FeatureDBClient) backgroundCheckVpcAddress() {
	ticker := time.NewTicker(f.checkInterval)
	defer ticker.Stop()
	for {
		select {
		case <-f.stopChan:
			return
		case <-ticker.C:
			f.CheckVpcAddress()
		}
	}
}

func (f *FeatureDBClient) CheckVpcAddress() {
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/health", f.vpcAddress), nil)
	if err == nil {
		req.Header.Set("Content-Type", "application/json")
		resp, err := f.Client.Do(req)
		if err == nil && resp.StatusCode == http.StatusOK {
			f.useVpcAddress.Store(true)
			return
		}
	}

	f.useVpcAddress.Store(false)
}

func (f *FeatureDBClient) GetCurrentAddress(check bool) string {
	if f.vpcAddress == "" {
		return f.address
	}

	if check {
		f.CheckVpcAddress()
	}

	if f.useVpcAddress.Load() {
		return f.vpcAddress
	} else {
		return f.address
	}
}

func (f *FeatureDBClient) Stop() {
	close(f.stopChan)
}
