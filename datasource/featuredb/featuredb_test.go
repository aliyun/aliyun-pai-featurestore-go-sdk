package featuredb

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestInitFeatureDBClientConcurrent(t *testing.T) {
	resetFeatureDBClientForTest(t)
	const goroutines = 64

	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	for range goroutines {
		go func() {
			defer wg.Done()
			InitFeatureDBClient("db.example.com", "token", "", true)
		}()
		go func() {
			defer wg.Done()
			_, _ = GetFeatureDBClient()
		}()
	}

	wg.Wait()

	client, err := GetFeatureDBClient()
	if err != nil {
		t.Fatalf("expected client after init, got error: %v", err)
	}
	if client == nil {
		t.Fatal("expected non-nil client after init")
	}
	if client.GetNormalAddress() != "db.example.com" {
		t.Fatalf("unexpected address: %q", client.GetNormalAddress())
	}
	if client.Token != "token" {
		t.Fatalf("unexpected token: %q", client.Token)
	}

	InitFeatureDBClient("other.example.com", "other-token", "", true)
	client2, err := GetFeatureDBClient()
	if err != nil {
		t.Fatalf("unexpected error on second get: %v", err)
	}
	if client2 != client {
		t.Fatal("expected singleton to remain the first initialized client")
	}
}

func TestFeatureDBAddressSelection(t *testing.T) {
	for _, tc := range []struct {
		name       string
		statuses   []int
		incomplete bool
		wantPVL    bool
		wantChecks int32
	}{
		{name: "healthy", statuses: []int{200}, wantPVL: true, wantChecks: 1},
		{name: "recovers", statuses: []int{503, 503, 200}, wantPVL: true, wantChecks: 3},
		{name: "4xx", statuses: []int{403}, wantChecks: 1},
		{name: "5xx", statuses: []int{503}, wantChecks: 3},
		{name: "incomplete_body", statuses: []int{200}, incomplete: true, wantChecks: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resetFeatureDBClientForTest(t)
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				index := int(requests.Add(1)) - 1
				if tc.incomplete {
					w.Header().Set("Content-Length", "10")
					_, _ = io.WriteString(w, "short")
					return
				}
				w.WriteHeader(tc.statuses[min(index, len(tc.statuses)-1)])
			}))
			defer server.Close()

			const normalAddress = "http://normal.example.com"
			InitFeatureDBClient(normalAddress, "token", strings.TrimPrefix(server.URL, "http://"), false)
			client, err := GetFeatureDBClient()
			if err != nil {
				t.Fatal(err)
			}
			want := normalAddress
			if tc.wantPVL {
				want = server.URL
			}
			server.Close()
			client.CheckVpcAddress()
			client.Stop()
			for _, check := range []bool{false, true} {
				if got := client.GetCurrentAddress(check); got != want {
					t.Fatalf("expected fixed address %q, got %q", want, got)
				}
			}
			if got := requests.Load(); got != tc.wantChecks {
				t.Fatalf("expected %d initialization checks only, got %d", tc.wantChecks, got)
			}
			if client.Client.Timeout != 0 {
				t.Fatalf("business client timeout changed: %s", client.Client.Timeout)
			}
			transport := client.Client.Transport.(*http.Transport)
			if transport.ResponseHeaderTimeout != 500*time.Millisecond || transport.MaxConnsPerHost != 1000 {
				t.Fatal("business transport settings changed")
			}
		})
	}
}

func TestFeatureDBHealthTimeout(t *testing.T) {
	resetFeatureDBClientForTest(t)
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()

	const normalAddress = "http://normal.example.com"
	start := time.Now()
	InitFeatureDBClient(normalAddress, "token", strings.TrimPrefix(server.URL, "http://"), false)
	client, err := GetFeatureDBClient()
	if err != nil {
		t.Fatal(err)
	}
	if client.GetCurrentAddress(false) != normalAddress || requests.Load() != vpcHealthCheckAttempts {
		t.Fatal("expected normal address after three health timeouts")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("health checks took %s", elapsed)
	}
}

func resetFeatureDBClientForTest(t *testing.T) {
	t.Helper()
	reset := func() {
		client := featureDBClient.Swap(nil)
		if client != nil {
			client.Client.CloseIdleConnections()
		}
	}
	reset()
	t.Cleanup(reset)
}
