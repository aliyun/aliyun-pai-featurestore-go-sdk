package featuredb

import (
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"
)

type FeatureDBClient struct {
	Client     *http.Client
	address    string
	Token      string
	vpcAddress string

	CurrentAddress string
	UseVpcAddress  bool
	AddressMutex   sync.RWMutex
	checkInterval  time.Duration
	stopChan       chan struct{}
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
		Client:         client,
		address:        address,
		Token:          token,
		vpcAddress:     fmt.Sprintf("http://%s", vpcAddress),
		CurrentAddress: address,
		UseVpcAddress:  false,
		checkInterval:  1 * time.Minute,
		stopChan:       make(chan struct{}),
	}

	if vpcAddress != "" {
		featureDBClient.CheckVpcAddress(1)

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
			f.CheckVpcAddress(1)
		}
	}
}

func (f *FeatureDBClient) CheckVpcAddress(maxTryCount int) {
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/health", f.vpcAddress), nil)
	if err == nil {
		req.Header.Set("Content-Type", "application/json")
		retryCount := 0
		for retryCount < maxTryCount {
			resp, err := f.Client.Do(req)
			if err == nil && resp.StatusCode == http.StatusOK {
				f.AddressMutex.Lock()
				f.CurrentAddress = f.vpcAddress
				f.UseVpcAddress = true
				f.AddressMutex.Unlock()
				return
			}
			retryCount++
		}
	}

	f.AddressMutex.Lock()
	f.CurrentAddress = f.address
	f.UseVpcAddress = false
	f.AddressMutex.Unlock()
}

func (f *FeatureDBClient) Stop() {
	close(f.stopChan)
}
