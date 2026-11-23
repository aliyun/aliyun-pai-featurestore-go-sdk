package dao

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/api"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/datasource/featuredb"
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

func TestFeatureDBOnlineReadReturnsHTTPError(t *testing.T) {
	var normalRequests, selectedRequests atomic.Int32
	normalServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		normalRequests.Add(1)
		if strings.HasSuffix(r.URL.Path, "/snapshots") {
			_, _ = io.WriteString(w, `{"data":{"snapshot_id":"snapshot","ts":1}}`)
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, `{"message":"unavailable"}`)
	}))
	defer normalServer.Close()
	selectedServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			return
		}
		selectedRequests.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, `{"message":"unavailable"}`)
	}))
	defer selectedServer.Close()
	featuredb.InitFeatureDBClient(normalServer.URL, "token", strings.TrimPrefix(selectedServer.URL, "http://"), false)
	client, err := featuredb.GetFeatureDBClient()
	if err != nil {
		t.Fatal(err)
	}
	d := &FeatureViewFeatureDBDao{
		featureDBClient: client,
		database:        "database",
		schema:          "schema",
		table:           "table",
		signature:       "signature",
		primaryKeyField: "id",
	}
	ctx := context.Background()
	keys := []interface{}{"user"}
	seqConfig := api.FeatureViewSeqConfig{}
	onlineConfig := []*api.SeqConfig{{SeqEvent: "click", SeqLen: 1, OnlineSeqName: "sequence"}}
	for _, tc := range []struct {
		name           string
		read           func() error
		normalRequests int32
	}{
		{name: "kv", read: func() error {
			_, err := d.GetFeaturesWithContext(ctx, keys, nil, 1)
			return err
		}},
		{name: "sequence", read: func() error {
			_, err := d.GetUserSequenceFeatureWithContext(ctx, keys, "user_id", seqConfig, onlineConfig)
			return err
		}},
		{name: "aggregated_sequence", read: func() error {
			_, err := d.GetUserAggregatedSequenceFeatureWithContext(ctx, keys, "user_id", seqConfig, onlineConfig)
			return err
		}},
		{name: "behavior_scan", read: func() error {
			_, err := d.GetUserBehaviorFeatureWithContext(ctx, keys, nil, nil, seqConfig)
			return err
		}},
		{name: "behavior_batch", read: func() error {
			_, err := d.GetUserBehaviorFeatureWithContext(ctx, keys, []interface{}{"click"}, nil, seqConfig)
			return err
		}},
		{name: "direct_write", read: func() error {
			return d.WriteFeaturesDirect([]map[string]interface{}{{"id": "user"}})
		}},
		{name: "write", normalRequests: 1, read: func() error {
			return d.writeFeatureDB([]map[string]interface{}{{"id": "user"}})
		}},
		{name: "snapshot_scan", normalRequests: 2, read: func() error {
			_, _, err := d.RowCountIds("")
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			beforeNormal, beforeSelected := normalRequests.Load(), selectedRequests.Load()
			err := tc.read()
			if err == nil || !strings.Contains(err.Error(), "503") || !strings.Contains(err.Error(), "unavailable") {
				t.Fatalf("expected status and response message in error, got %v", err)
			}
			wantSelected := int32(0)
			if tc.normalRequests == 0 {
				wantSelected = 1
			}
			if got := normalRequests.Load() - beforeNormal; got != tc.normalRequests {
				t.Fatalf("expected %d normal address requests, got %d", tc.normalRequests, got)
			}
			if got := selectedRequests.Load() - beforeSelected; got != wantSelected {
				t.Fatalf("expected %d selected address requests, got %d", wantSelected, got)
			}
		})
	}
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
