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

// featureDBClient is a process-wide singleton. It is accessed concurrently
// (e.g. GetLLMConfig fans out per-name via singleflight), so it is stored in an
// atomic.Pointer to make reads lock-free and initialization race-free.
var (
	featureDBClient atomic.Pointer[FeatureDBClient]
)

func InitFeatureDBClient(address, token, vpcAddress string, isTestMode bool) {
	// Fast path: already initialized by a previous caller.
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
		Client:        client,
		address:       address,
		Token:         token,
		vpcAddress:    fmt.Sprintf("http://%s", vpcAddress),
		checkInterval: 1 * time.Minute,
		stopChan:      make(chan struct{}),
	}

	newClient.useVpcAddress.Store(false)

	// Only the first caller wins the swap, guaranteeing the singleton is
	// initialized once and the background goroutine is started at most once,
	// even when multiple goroutines call InitFeatureDBClient concurrently.
	if !featureDBClient.CompareAndSwap(nil, newClient) {
		return
	}

	if vpcAddress != "" {
		newClient.CheckVpcAddress()

		go newClient.backgroundCheckVpcAddress()
	}
}

func GetFeatureDBClient() (*FeatureDBClient, error) {
	client := featureDBClient.Load()
	if client == nil {
		return nil, fmt.Errorf("FeatureDB has not been provisioned")
	}

	return client, nil
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
	if err != nil {
		f.useVpcAddress.Store(false)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := f.Client.Do(req)
	if err != nil {
		f.useVpcAddress.Store(false)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		f.useVpcAddress.Store(true)
		return
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

func (f *FeatureDBClient) GetNormalAddress() string {
	return f.address
}

func (f *FeatureDBClient) Stop() {
	close(f.stopChan)
}
