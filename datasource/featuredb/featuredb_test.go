package featuredb

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
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
			// Keep the server running so the request counter below stays
			// authoritative: if any post-init health check is reintroduced,
			// it would hit the live handler and inflate the count.
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

func TestFeatureDBReadClientTimeout(t *testing.T) {
	for _, tc := range []struct {
		name     string
		testMode bool
		timeout  time.Duration
	}{
		{name: "production", timeout: 500 * time.Millisecond},
		{name: "test", testMode: true, timeout: time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resetFeatureDBClientForTest(t)
			InitFeatureDBClient("http://normal.example.com", "token", "", tc.testMode)
			client, err := GetFeatureDBClient()
			if err != nil {
				t.Fatal(err)
			}
			if client.Client.Transport != client.readClient.Transport {
				t.Fatal("expected clients to share one transport")
			}
			if client.readClient.Timeout != tc.timeout {
				t.Fatalf("expected read timeout %s, got %s", tc.timeout, client.readClient.Timeout)
			}
			if client.Client.Timeout != 0 {
				t.Fatalf("non-read client timeout changed: %s", client.Client.Timeout)
			}
		})
	}
}

func TestDoReadRetriesSameRequest(t *testing.T) {
	var requests atomic.Int32
	var bodies []string
	var urls []string
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(req.Body)
		req.Body.Close()
		if err != nil {
			t.Errorf("read request body: %v", err)
		}
		bodies = append(bodies, string(body))
		urls = append(urls, req.URL.String())
		if req.Header.Get("Auth") != "signature" || req.Header.Get("X-FeatureView-Weight") != "2" {
			t.Error("request headers were not preserved")
		}
		if requests.Add(1) == 1 {
			return nil, &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("")),
			Header:     make(http.Header),
			Request:    req,
		}, nil
	})

	client := &FeatureDBClient{readClient: &http.Client{Transport: transport, Timeout: 500 * time.Millisecond}}
	req, err := http.NewRequest(http.MethodPost, "http://featuredb.example.com/read", bytes.NewReader([]byte("payload")))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Auth", "signature")
	req.Header.Set("X-FeatureView-Weight", "2")
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

func TestDoReadErrorPolicy(t *testing.T) {
	for _, tc := range []struct {
		name     string
		err      error
		attempts int32
	}{
		{name: "eof", err: io.EOF, attempts: 2},
		{name: "unexpected_eof", err: io.ErrUnexpectedEOF, attempts: 2},
		{name: "reset", err: &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}, attempts: 2},
		{name: "broken_pipe", err: &net.OpError{Op: "write", Net: "tcp", Err: syscall.EPIPE}, attempts: 2},
		{name: "timeout", err: &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ETIMEDOUT}, attempts: 1},
		{name: "dns_timeout", err: &net.DNSError{Err: "timeout", Name: "featuredb", IsTimeout: true}, attempts: 1},
		{name: "dns_not_found", err: &net.DNSError{Err: "not found", Name: "featuredb", IsNotFound: true}, attempts: 1},
		{name: "connection_refused", err: syscall.ECONNREFUSED, attempts: 1},
		{name: "deadline", err: context.DeadlineExceeded, attempts: 1},
		{name: "canceled", err: context.Canceled, attempts: 1},
		{name: "unknown", err: errors.New("unknown transport error"), attempts: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var requests atomic.Int32
			client := &FeatureDBClient{readClient: &http.Client{
				Timeout: 500 * time.Millisecond,
				Transport: roundTripFunc(func(_ *http.Request) (*http.Response, error) {
					requests.Add(1)
					return nil, tc.err
				}),
			}}
			req, err := http.NewRequest(http.MethodGet, "http://featuredb.example.com", nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := client.DoRead(req); !errors.Is(err, tc.err) {
				t.Fatalf("expected original error %v, got %v", tc.err, err)
			}
			if got := requests.Load(); got != tc.attempts {
				t.Fatalf("expected %d attempts, got %d", tc.attempts, got)
			}
		})
	}
}

func TestShouldRetryReadHalfTimeoutBoundary(t *testing.T) {
	for _, timeout := range []time.Duration{500 * time.Millisecond, time.Second} {
		threshold := timeout / 2
		if !shouldRetryRead(io.EOF, threshold-time.Nanosecond, timeout) {
			t.Fatalf("expected retry before half of %s", timeout)
		}
		if shouldRetryRead(io.EOF, threshold, timeout) {
			t.Fatalf("must not retry at half of %s", timeout)
		}
		if shouldRetryRead(io.EOF, threshold+time.Nanosecond, timeout) {
			t.Fatalf("must not retry after half of %s", timeout)
		}
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
	if _, err := client.DoRead(req); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline error, got %v", err)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("expected no retry, got %d requests", got)
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
	if got := requests.Load(); got != 1 {
		t.Fatalf("expected one attempt without retry, got %d", got)
	}
	elapsed := time.Since(start)
	t.Logf("one attempt waiting for headers: %s", elapsed)
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
	var attempts atomic.Int32
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), &httptrace.ClientTrace{
		GetConn: func(string) { attempts.Add(1) },
	}))
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
	if got := attempts.Load(); got != 1 {
		t.Fatalf("expected one queued attempt without retry, got %d", got)
	}
	elapsed := time.Since(start)
	t.Logf("waiting for a connection without retry: %s", elapsed)
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
