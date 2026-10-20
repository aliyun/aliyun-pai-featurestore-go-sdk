package dao

import (
	"sync"
	"testing"
	"time"
)

// newTestFeatureDBDao builds a FeatureViewFeatureDBDao with only the async
// write machinery initialized, WITHOUT starting the background goroutine
// (which is now started lazily on the first WriteFeatures call). It avoids the
// real FeatureDB client so the tests stay hermetic; as long as writeData is
// empty the background goroutine never issues a network write.
func newTestFeatureDBDao() *FeatureViewFeatureDBDao {
	return &FeatureViewFeatureDBDao{
		writeData: make([]map[string]interface{}, 0, 100),
		batchSize: 20,
		stopChan:  make(chan struct{}),
		done:      make(chan struct{}),
	}
}

// TestFeatureDBDaoNoGoroutineUntilWrite verifies a DAO that is never written
// to does not start the background goroutine, and that Close returns promptly
// without waiting on a goroutine that was never started.
func TestFeatureDBDaoNoGoroutineUntilWrite(t *testing.T) {
	d := newTestFeatureDBDao()

	d.mu.Lock()
	started := d.started
	d.mu.Unlock()
	if started {
		t.Fatal("goroutine should not be started before any WriteFeatures call")
	}

	done := make(chan struct{})
	go func() {
		d.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close hung although no background goroutine was started")
	}
}

// TestFeatureDBDaoLazyStartAndClose verifies the goroutine is started on the
// first write and that Close joins it (done closed) on return.
func TestFeatureDBDaoLazyStartAndClose(t *testing.T) {
	d := newTestFeatureDBDao()

	// An empty write still starts the goroutine but buffers no rows, so the
	// ticker never triggers a (networked) flush — keeping the test hermetic.
	d.WriteFeatures([]map[string]interface{}{})

	d.mu.Lock()
	started := d.started
	d.mu.Unlock()
	if !started {
		t.Fatal("goroutine should be started after first WriteFeatures call")
	}

	d.Close()

	select {
	case <-d.done:
	default:
		t.Fatal("Close returned before the background goroutine exited")
	}
}

// TestFeatureDBDaoWriteAfterCloseDiscarded verifies writes after Close are
// discarded and do not start a goroutine.
func TestFeatureDBDaoWriteAfterCloseDiscarded(t *testing.T) {
	d := newTestFeatureDBDao()
	d.Close()

	d.WriteFeatures([]map[string]interface{}{{"id": "x"}})

	d.mu.Lock()
	buffered := len(d.writeData)
	started := d.started
	d.mu.Unlock()
	if buffered != 0 {
		t.Fatalf("expected writes after Close to be discarded, got %d buffered", buffered)
	}
	if started {
		t.Fatal("writes after Close must not start the background goroutine")
	}
}

// TestFeatureDBDaoCloseIdempotent verifies Close is safe to call multiple
// times, including concurrently, without panicking (double close of stopChan)
// or deadlocking.
func TestFeatureDBDaoCloseIdempotent(t *testing.T) {
	d := newTestFeatureDBDao()
	d.WriteFeatures([]map[string]interface{}{}) // start the goroutine

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
