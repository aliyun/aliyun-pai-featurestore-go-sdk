package dao

import (
	"sync"
	"testing"
	"time"
)

// newTestFeatureDBDao builds a FeatureViewFeatureDBDao with only the async
// write machinery initialized and starts its background goroutine. It avoids
// the real FeatureDB client so the tests stay hermetic; as long as writeData
// is empty the background goroutine never issues a network write.
func newTestFeatureDBDao() *FeatureViewFeatureDBDao {
	d := &FeatureViewFeatureDBDao{
		writeData:   make([]map[string]interface{}, 0, 100),
		batchSize:   20,
		stopChan:    make(chan struct{}),
		done:        make(chan struct{}),
		flushTicker: time.NewTicker(50 * time.Millisecond),
	}
	go d.startAsyncWrite()
	return d
}

// TestFeatureDBDaoCloseJoinsGoroutine verifies that Close does not return
// until the background async-write goroutine has fully exited (done closed),
// which is the core guarantee added to fix the "Close returns while a write
// is still in flight" defect.
func TestFeatureDBDaoCloseJoinsGoroutine(t *testing.T) {
	d := newTestFeatureDBDao()
	// let the ticker spin a few times
	time.Sleep(120 * time.Millisecond)

	d.Close()

	// On return the goroutine must already have exited: done is closed.
	select {
	case <-d.done:
	default:
		t.Fatal("Close returned before the background goroutine exited")
	}

	// closed flag must be set and further writes discarded.
	d.mu.Lock()
	closed := d.closed
	d.mu.Unlock()
	if !closed {
		t.Fatal("expected closed=true after Close")
	}

	d.WriteFeatures([]map[string]interface{}{{"id": "x"}})
	d.mu.Lock()
	buffered := len(d.writeData)
	d.mu.Unlock()
	if buffered != 0 {
		t.Fatalf("expected writes after Close to be discarded, got %d buffered", buffered)
	}
}

// TestFeatureDBDaoCloseIdempotent verifies Close is safe to call multiple
// times, including concurrently, without panicking (double close of stopChan)
// or deadlocking.
func TestFeatureDBDaoCloseIdempotent(t *testing.T) {
	d := newTestFeatureDBDao()

	const callers = 8
	var wg sync.WaitGroup
	wg.Add(callers)
	for i := 0; i < callers; i++ {
		go func() {
			defer wg.Done()
			d.Close()
		}()
	}

	waitTimeout(t, &wg, 2*time.Second, "concurrent Close calls did not all return")

	select {
	case <-d.done:
	default:
		t.Fatal("background goroutine did not exit after concurrent Close")
	}
}

// TestFeatureDBDaoWriteFlushThenClose verifies WriteFlush and Close share the
// same idempotent shutdown path and can be combined without deadlock.
func TestFeatureDBDaoWriteFlushThenClose(t *testing.T) {
	d := newTestFeatureDBDao()
	time.Sleep(60 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		d.WriteFlush()
		d.Close()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("WriteFlush followed by Close deadlocked")
	}
}

// TestFeatureDBDaoCloseNilReceiver verifies Close tolerates a nil receiver,
// which can arise when construction fails and a typed-nil DAO is later closed
// during a project-data refresh.
func TestFeatureDBDaoCloseNilReceiver(t *testing.T) {
	var d *FeatureViewFeatureDBDao
	d.Close() // must not panic
}

func waitTimeout(t *testing.T, wg *sync.WaitGroup, timeout time.Duration, msg string) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatal(msg)
	}
}
