package featuredb

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

const (
	vpcHealthCheckAttempts = 3
	vpcHealthCheckInterval = 50 * time.Millisecond
)

type FeatureDBClient struct {
	Client     *http.Client
	address    string
	Token      string
	vpcAddress string

	selectedAddress string
}

var (
	featureDBClient atomic.Pointer[FeatureDBClient]
	initMu          sync.Mutex
)

func InitFeatureDBClient(address, token, vpcAddress string, isTestMode bool) {
	if featureDBClient.Load() != nil {
		return
	}

	initMu.Lock()
	defer initMu.Unlock()
	if featureDBClient.Load() != nil {
		return
	}

	dialTimeout := 200 * time.Millisecond
	responseTimeout := 500 * time.Millisecond
	if isTestMode {
		dialTimeout = 1000 * time.Millisecond
		responseTimeout = 1000 * time.Millisecond
	}

	client := &http.Client{
		Transport: &http.Transport{
			MaxConnsPerHost:     1000,
			MaxIdleConns:        1000,
			MaxIdleConnsPerHost: 1000,
			DialContext: (&net.Dialer{
				Timeout:   dialTimeout,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			ResponseHeaderTimeout: responseTimeout,
			IdleConnTimeout:       90 * time.Second,
		},
	}
	newClient := &FeatureDBClient{
		Client:          client,
		address:         address,
		Token:           token,
		vpcAddress:      fmt.Sprintf("http://%s", vpcAddress),
		selectedAddress: address,
	}

	if vpcAddress != "" {
		if err := newClient.selectVpcAddress(responseTimeout); err != nil {
			log.Printf("FeatureDB uses normal address because PVL health check failed: %v", err)
		} else {
			log.Printf("FeatureDB uses PVL address")
		}
	} else {
		log.Printf("FeatureDB uses normal address")
	}

	featureDBClient.Store(newClient)
}

func GetFeatureDBClient() (*FeatureDBClient, error) {
	client := featureDBClient.Load()
	if client == nil {
		return nil, fmt.Errorf("FeatureDB has not been provisioned")
	}

	return client, nil
}

func (f *FeatureDBClient) selectVpcAddress(timeout time.Duration) error {
	healthClient := &http.Client{Transport: f.Client.Transport, Timeout: timeout}
	var lastErr error
	for attempt := 0; attempt < vpcHealthCheckAttempts; attempt++ {
		var retryable bool
		retryable, lastErr = f.checkVpcAddress(healthClient)
		if lastErr == nil {
			f.selectedAddress = f.vpcAddress
			return nil
		}
		if !retryable || attempt == vpcHealthCheckAttempts-1 {
			break
		}
		time.Sleep(vpcHealthCheckInterval)
	}
	return lastErr
}

func (f *FeatureDBClient) checkVpcAddress(client *http.Client) (bool, error) {
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/health", f.vpcAddress), nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return true, err
	}
	defer resp.Body.Close()
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		return true, err
	}
	if resp.StatusCode != http.StatusOK {
		return resp.StatusCode >= http.StatusInternalServerError, fmt.Errorf("status code: %d", resp.StatusCode)
	}
	return false, nil
}

func (f *FeatureDBClient) GetCurrentAddress(_ bool) string {
	return f.selectedAddress
}

func (f *FeatureDBClient) GetNormalAddress() string {
	return f.address
}

// Deprecated: the address is selected during initialization and remains fixed.
func (f *FeatureDBClient) CheckVpcAddress() {}

// Deprecated: FeatureDBClient no longer starts a background health check.
func (f *FeatureDBClient) Stop() {}
