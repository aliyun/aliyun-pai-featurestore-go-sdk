package featuredb

import (
	"context"
	"errors"
	"io"
	"net"
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

func TestInitFeatureDBClientSelectsVpcAfterRetry(t *testing.T) {
	resetFeatureDBClientForTest(t)
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if requests.Add(1) < vpcHealthCheckAttempts {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	InitFeatureDBClient("http://normal.example.com", "token", strings.TrimPrefix(server.URL, "http://"), false)
	client, err := GetFeatureDBClient()
	if err != nil {
		t.Fatal(err)
	}
	if got := client.GetCurrentAddress(false); got != server.URL {
		t.Fatalf("expected PVL address %q, got %q", server.URL, got)
	}
	if got := requests.Load(); got != vpcHealthCheckAttempts {
		t.Fatalf("expected %d health checks, got %d", vpcHealthCheckAttempts, got)
	}
}

func TestInitFeatureDBClientFallsBackWithoutRetryOn4xx(t *testing.T) {
	resetFeatureDBClientForTest(t)
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusForbidden)
	}))
	defer server.Close()

	const normalAddress = "http://normal.example.com"
	InitFeatureDBClient(normalAddress, "token", strings.TrimPrefix(server.URL, "http://"), false)
	client, err := GetFeatureDBClient()
	if err != nil {
		t.Fatal(err)
	}
	if got := client.GetCurrentAddress(true); got != normalAddress {
		t.Fatalf("expected normal address %q, got %q", normalAddress, got)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("expected one health check, got %d", got)
	}
}

func TestInitFeatureDBClientRejectsIncompleteHealthBody(t *testing.T) {
	resetFeatureDBClientForTest(t)
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Length", "10")
		_, _ = io.WriteString(w, "short")
	}))
	defer server.Close()

	const normalAddress = "http://normal.example.com"
	InitFeatureDBClient(normalAddress, "token", strings.TrimPrefix(server.URL, "http://"), false)
	client, err := GetFeatureDBClient()
	if err != nil {
		t.Fatal(err)
	}
	if got := client.GetCurrentAddress(false); got != normalAddress {
		t.Fatalf("expected normal address after truncated health responses, got %q", got)
	}
	if got := requests.Load(); got != vpcHealthCheckAttempts {
		t.Fatalf("expected %d health checks, got %d", vpcHealthCheckAttempts, got)
	}
}

func TestFeatureDBReadClientSharesTransportAndHasTimeout(t *testing.T) {
	resetFeatureDBClientForTest(t)
	InitFeatureDBClient("http://normal.example.com", "token", "", false)
	client, err := GetFeatureDBClient()
	if err != nil {
		t.Fatal(err)
	}
	if client.Client.Transport != client.readClient.Transport {
		t.Fatal("expected clients to share one transport")
	}
	if client.readClient.Timeout != 500*time.Millisecond {
		t.Fatalf("expected 500ms read timeout, got %s", client.readClient.Timeout)
	}
	if client.Client.Timeout != 0 {
		t.Fatalf("expected non-read client without total timeout, got %s", client.Client.Timeout)
	}
}

func TestDoReadRetriesSameRequest(t *testing.T) {
	var requests atomic.Int32
	var bodies []string
	var urls []string
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(req.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
		}
		bodies = append(bodies, string(body))
		urls = append(urls, req.URL.String())
		if requests.Add(1) == 1 {
			return nil, errors.New("temporary transport error")
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("")),
			Header:     make(http.Header),
			Request:    req,
		}, nil
	})

	client := &FeatureDBClient{readClient: &http.Client{Transport: transport}}
	req, err := http.NewRequest(http.MethodPost, "http://featuredb.example.com/read", strings.NewReader("payload"))
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.DoRead(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if got := requests.Load(); got != 2 {
		t.Fatalf("expected two requests, got %d", got)
	}
	if len(bodies) != 2 || bodies[0] != "payload" || bodies[1] != "payload" {
		t.Fatalf("unexpected request bodies: %#v", bodies)
	}
	if len(urls) != 2 || urls[0] != urls[1] {
		t.Fatalf("expected retries to use the same URL, got %#v", urls)
	}
}

func TestDoReadDoesNotRetryHTTPError(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	client := &FeatureDBClient{readClient: server.Client()}
	req, err := http.NewRequest(http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.DoRead(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected status 503, got %d", response.StatusCode)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("expected no retry, got %d requests", got)
	}
}

func TestDoReadRetriesClientTimeout(t *testing.T) {
	var requests atomic.Int32
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		requests.Add(1)
		<-req.Context().Done()
		return nil, req.Context().Err()
	})
	client := &FeatureDBClient{readClient: &http.Client{
		Transport: transport,
		Timeout:   20 * time.Millisecond,
	}}
	req, err := http.NewRequest(http.MethodGet, "http://featuredb.example.com", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.DoRead(req); err == nil {
		t.Fatal("expected read timeout")
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("expected one retry, got %d requests", got)
	}
}

func TestDoReadDoesNotRetryExpiredContext(t *testing.T) {
	var requests atomic.Int32
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		requests.Add(1)
		<-req.Context().Done()
		return nil, req.Context().Err()
	})
	client := &FeatureDBClient{readClient: &http.Client{Transport: transport}}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://featuredb.example.com", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.DoRead(req); err == nil {
		t.Fatal("expected context deadline error")
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("expected no retry, got %d requests", got)
	}
}

func TestInitFeatureDBClientKeepsAddressAfterHealthChanges(t *testing.T) {
	for _, initialStatus := range []int{http.StatusOK, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(initialStatus), func(t *testing.T) {
			resetFeatureDBClientForTest(t)
			var status atomic.Int32
			status.Store(int32(initialStatus))
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				requests.Add(1)
				w.WriteHeader(int(status.Load()))
			}))
			defer server.Close()

			const normalAddress = "http://normal.example.com"
			InitFeatureDBClient(normalAddress, "token", strings.TrimPrefix(server.URL, "http://"), false)
			client, err := GetFeatureDBClient()
			if err != nil {
				t.Fatal(err)
			}
			want := normalAddress
			wantChecks := int32(vpcHealthCheckAttempts)
			if initialStatus == http.StatusOK {
				want = server.URL
				wantChecks = 1
				status.Store(http.StatusServiceUnavailable)
			} else {
				status.Store(http.StatusOK)
			}
			client.CheckVpcAddress()
			if got := client.GetCurrentAddress(true); got != want {
				t.Fatalf("expected fixed address %q, got %q", want, got)
			}
			if got := requests.Load(); got != wantChecks {
				t.Fatalf("expected %d initialization checks only, got %d", wantChecks, got)
			}
		})
	}
}

func TestDoReadTimeoutWaitingForHeaders(t *testing.T) {
	resetFeatureDBClientForTest(t)
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		<-r.Context().Done()
	}))
	defer server.Close()
	InitFeatureDBClient(server.URL, "token", "", false)
	client, err := GetFeatureDBClient()
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	response, err := client.DoRead(req)
	if response != nil {
		response.Body.Close()
	}
	var timeoutErr net.Error
	if !errors.As(err, &timeoutErr) || !timeoutErr.Timeout() {
		t.Fatalf("expected timeout, got %v", err)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("expected two attempts, got %d", got)
	}
	elapsed := time.Since(start)
	if elapsed > 3*time.Second {
		t.Fatalf("two 500ms attempts took %s", elapsed)
	}
	t.Logf("two attempts waiting for headers: %s", elapsed)
}

func TestDoReadTimeoutWaitingForConnection(t *testing.T) {
	resetFeatureDBClientForTest(t)
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()
	InitFeatureDBClient(server.URL, "token", "", false)
	client, err := GetFeatureDBClient()
	if err != nil {
		t.Fatal(err)
	}
	transport := client.Client.Transport.(*http.Transport)
	transport.MaxConnsPerHost = 1
	holdingResponse, err := client.Client.Get(server.URL + "/hold")
	if err != nil {
		t.Fatal(err)
	}
	defer holdingResponse.Body.Close()

	req, err := http.NewRequest(http.MethodGet, server.URL+"/queued", nil)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	response, err := client.DoRead(req)
	if response != nil {
		response.Body.Close()
	}
	var timeoutErr net.Error
	if !errors.As(err, &timeoutErr) || !timeoutErr.Timeout() {
		t.Fatalf("expected connection queue timeout, got %v", err)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("queued requests should not reach server, got %d requests", got)
	}
	elapsed := time.Since(start)
	if elapsed > 3*time.Second {
		t.Fatalf("two queued attempts took %s", elapsed)
	}
	t.Logf("two attempts waiting for a connection: %s", elapsed)
}

func TestReadTimeoutCoversBodyButNotNonReadClient(t *testing.T) {
	resetFeatureDBClientForTest(t)
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		timer := time.NewTimer(650 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
			_, _ = io.WriteString(w, "payload")
		case <-r.Context().Done():
		}
	}))
	defer server.Close()
	InitFeatureDBClient(server.URL, "token", "", false)
	client, err := GetFeatureDBClient()
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	response, err := client.DoRead(req)
	if err != nil {
		t.Fatal(err)
	}
	_, readErr := io.ReadAll(response.Body)
	response.Body.Close()
	var timeoutErr net.Error
	if !errors.As(readErr, &timeoutErr) || !timeoutErr.Timeout() {
		t.Fatalf("expected body timeout, got %v", readErr)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("body errors must not retry, got %d requests", got)
	}
	t.Logf("online body timeout: %s", time.Since(start))

	response, err = client.Client.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil || string(body) != "payload" {
		t.Fatalf("non-read body should finish after 500ms: body=%q, err=%v", body, err)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
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
