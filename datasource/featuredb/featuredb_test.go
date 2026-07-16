package featuredb

import (
	"sync"
	"testing"
)

// TestInitFeatureDBClientConcurrent verifies that concurrent calls to
// InitFeatureDBClient and GetFeatureDBClient are race-free and that the
// singleton is initialized exactly once. Run with -race to catch data races on
// the package-level singleton.
//
// vpcAddress is left empty on purpose so no background goroutine or HTTP health
// check is started, keeping the test hermetic.
func TestInitFeatureDBClientConcurrent(t *testing.T) {
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
			// Concurrent reads must never race with the initializing write.
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

	// A second Init with different args must be ignored (first caller wins).
	InitFeatureDBClient("other.example.com", "other-token", "", true)
	client2, err := GetFeatureDBClient()
	if err != nil {
		t.Fatalf("unexpected error on second get: %v", err)
	}
	if client2 != client {
		t.Fatal("expected singleton to remain the first initialized client")
	}
}
