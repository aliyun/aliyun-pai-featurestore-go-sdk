package dao

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/arrow"
	"github.com/aliyun/aliyun-odps-go-sdk/arrow/array"
	"github.com/aliyun/aliyun-odps-go-sdk/arrow/ipc"
	"github.com/aliyun/aliyun-odps-go-sdk/arrow/memory"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/constants"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/datasource/featuredb"
)

// TestMain initializes the process-wide FeatureDB client singleton once,
// pointing it at a fake FeatureDB that serves snapshot, scan and
// iterate_get_kv.
func TestMain(m *testing.M) {
	srv := httptest.NewServer(http.HandlerFunc(fakeFeatureDBHandler))
	featuredb.InitFeatureDBClient(srv.URL, "token", "", true)
	code := m.Run()
	srv.Close()
	os.Exit(code)
}

var (
	corruptIterateHits    atomic.Int64
	corruptRetryUsedOldTs atomic.Bool
)

func fakeFeatureDBHandler(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/slow-snapshot/snapshots"):
		<-r.Context().Done()
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/snapshots"):
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"code":"OK","data":{"snapshot_id":"s1","ts":1}}`)
	case strings.HasSuffix(r.URL.Path, "/scan"):
		writeFakeArrowStream(w)
	case strings.Contains(r.URL.Path, "/corrupt-iterate/iterate_get_kv"):
		w.Header().Set("Next-Ts", "2")
		if corruptIterateHits.Add(1) == 1 {
			io.WriteString(w, "invalid arrow stream")
			return
		}
		if r.URL.Query().Get("ts") == "1" {
			corruptRetryUsedOldTs.Store(true)
		}
		writeFakeArrowStream(w)
	case strings.HasSuffix(r.URL.Path, "/iterate_get_kv"):
		w.Header().Set("Next-Ts", "2")
		writeFakeArrowStream(w)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

// fakeFeatureFields and fakeFeatureTypes describe the table served by the fake
// FeatureDB.
var (
	fakeFeatureFields = []string{"v", "s", "a", "n", "m", "i8", "u8"}
	fakeFeatureTypes  = map[string]constants.FSType{
		"v":  constants.FS_INT64,
		"s":  constants.FS_STRING,
		"a":  constants.FS_ARRAY_INT64,
		"n":  constants.FS_DOUBLE,
		"m":  constants.FS_MAP_STRING_INT32,
		"i8": constants.FS_ARRAY_INT8,
		"u8": constants.FS_ARRAY_UINT8,
	}
)

// encodeFakeRow encodes a row in the FeatureDB F/1 protocol with
// v=1, s="hello", a=[1 2], n=null and m={"k":7}.
func encodeFakeRow() []byte {
	var buf bytes.Buffer
	buf.WriteByte(FeatureDB_Protocal_Version_F)
	buf.WriteByte(FeatureDB_IfNull_Flag_Version_1)
	w := func(v interface{}) { binary.Write(&buf, binary.LittleEndian, v) }
	// v
	buf.WriteByte(0)
	w(int64(1))
	// s
	buf.WriteByte(0)
	w(uint32(5))
	buf.WriteString("hello")
	// a
	buf.WriteByte(0)
	w(uint32(2))
	w(int64(1))
	w(int64(2))
	// n is null
	buf.WriteByte(1)
	// m: length, string array offsets and bytes, then values
	buf.WriteByte(0)
	w(uint32(1))
	w(uint32(0))
	w(uint32(1))
	buf.WriteString("k")
	w(int32(7))
	// i8
	buf.WriteByte(0)
	w(uint32(2))
	buf.Write([]byte{0xff, 2})
	// u8
	buf.WriteByte(0)
	w(uint32(2))
	buf.Write([]byte{1, 0xff})
	return buf.Bytes()
}

// writeFakeArrowStream writes 3 rows of (pk string, data binary) with ids
// id_0..id_2, each carrying encodeFakeRow as data.
func writeFakeArrowStream(w io.Writer) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "pk", Type: arrow.BinaryTypes.String},
		{Name: "data", Type: arrow.BinaryTypes.Binary},
	}, nil)
	b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer b.Release()

	data := encodeFakeRow()
	for i := 0; i < 3; i++ {
		b.Field(0).(*array.StringBuilder).Append(fmt.Sprintf("id_%d", i))
		b.Field(1).(*array.BinaryBuilder).Append(data)
	}
	rec := b.NewRecord()
	defer rec.Release()

	writer := ipc.NewWriter(w, ipc.WithSchema(schema))
	writer.Write(rec)
	writer.Close()
}

// iterateGoroutineAlive reports whether the iterateFeatures background
// goroutine is still present in the goroutine dump.
func iterateGoroutineAlive() bool {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	return strings.Contains(string(buf[:n]), "(*FeatureViewFeatureDBDao).iterateFeatures")
}

func newFakeTableDao(t *testing.T) *FeatureViewFeatureDBDao {
	t.Helper()
	return newFakeTableDaoForTable(t, "table")
}

func newFakeTableDaoForTable(t *testing.T, table string) *FeatureViewFeatureDBDao {
	t.Helper()
	d := NewFeatureViewFeatureDBDao(DaoConfig{
		FeatureDBDatabaseName: "db",
		FeatureDBSchemaName:   "schema",
		FeatureDBTableName:    table,
		PrimaryKeyField:       "pk",
		Fields:                fakeFeatureFields,
		FieldTypeMap:          fakeFeatureTypes,
	})
	if d == nil {
		t.Fatal("FeatureDB client is not initialized")
	}
	return d
}

// TestDecodeAllFeatures verifies every field is decoded, null fields are
// omitted and truncated or unsupported data is rejected.
func TestDecodeAllFeatures(t *testing.T) {
	d := &FeatureViewFeatureDBDao{fields: fakeFeatureFields, fieldTypeMap: fakeFeatureTypes}
	data := encodeFakeRow()

	got, err := d.decodeAllFeatures(data)
	if err != nil {
		t.Fatalf("decodeAllFeatures: %v", err)
	}
	want := map[string]interface{}{
		"v":  int64(1),
		"s":  "hello",
		"a":  []int64{1, 2},
		"m":  map[string]int32{"k": 7},
		"i8": []int8{-1, 2},
		"u8": []uint8{1, 255},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("decodeAllFeatures = %#v, want %#v", got, want)
	}

	if _, err := d.decodeAllFeatures(data[:len(data)-2]); err == nil {
		t.Fatal("expected error for truncated data")
	}
	bad := append([]byte{'X', '1'}, data[2:]...)
	if _, err := d.decodeAllFeatures(bad); err == nil {
		t.Fatal("expected error for unsupported protocol version")
	}
}

// TestScanAndIterateFeaturesWithContext verifies the full scan calls fn
// synchronously for every row with all features and the primary key, the
// incremental goroutine keeps calling fn, and it exits within 1s after ctx is
// canceled.
func TestScanAndIterateFeaturesWithContext(t *testing.T) {
	origInterval := iterateFeaturesPollInterval
	iterateFeaturesPollInterval = 10 * time.Millisecond
	t.Cleanup(func() { iterateFeaturesPollInterval = origInterval })

	d := newFakeTableDao(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mu sync.Mutex
	var calls []map[string]interface{}
	err := d.ScanAndIterateFeaturesWithContext(ctx, func(id string, properties map[string]interface{}) {
		if properties["pk"] != id {
			t.Errorf("properties[pk] = %v, want %s", properties["pk"], id)
		}
		mu.Lock()
		calls = append(calls, properties)
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("ScanAndIterateFeaturesWithContext: %v", err)
	}

	mu.Lock()
	scanned := len(calls)
	first := calls[0]
	mu.Unlock()
	if scanned < 3 {
		t.Fatalf("expected fn to be called for 3 scanned rows before return, got %d", scanned)
	}
	if first["pk"] != "id_0" || first["s"] != "hello" || !reflect.DeepEqual(first["a"], []int64{1, 2}) {
		t.Fatalf("unexpected first row: %#v", first)
	}

	// Incremental rows keep arriving from the background goroutine.
	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		n := len(calls)
		mu.Unlock()
		if n > scanned {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("fn was never called with incremental rows")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !iterateGoroutineAlive() {
		t.Fatal("iterate goroutine should be running before cancel")
	}

	cancel()
	deadline = time.Now().Add(time.Second)
	for iterateGoroutineAlive() {
		if time.Now().After(deadline) {
			t.Fatal("iterate goroutine did not exit within 1s after cancel")
		}
		time.Sleep(10 * time.Millisecond)
	}

	mu.Lock()
	afterExit := len(calls)
	mu.Unlock()
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if len(calls) != afterExit {
		t.Fatal("fn was called after the iterate goroutine exited")
	}
}

// TestScanAndIterateFeaturesWithContextCanceled verifies a canceled ctx makes
// the full scan fail without starting the iterate goroutine.
func TestScanAndIterateFeaturesWithContextCanceled(t *testing.T) {
	d := newFakeTableDao(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	called := false
	err := d.ScanAndIterateFeaturesWithContext(ctx, func(string, map[string]interface{}) { called = true })
	if err == nil {
		t.Fatal("expected error for canceled ctx")
	}
	if called {
		t.Fatal("fn must not be called when ctx is canceled")
	}
	if iterateGoroutineAlive() {
		t.Fatal("iterate goroutine must not start when the scan fails")
	}

	if err := d.ScanAndIterateFeaturesWithContext(context.Background(), nil); err == nil {
		t.Fatal("expected error for nil fn")
	}
}

// TestScanAndIterateFeaturesWithContextCancelsSnapshot verifies cancellation
// interrupts an in-flight snapshot request before scan or iteration starts.
func TestScanAndIterateFeaturesWithContextCancelsSnapshot(t *testing.T) {
	d := newFakeTableDaoForTable(t, "slow-snapshot")
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := d.ScanAndIterateFeaturesWithContext(ctx, func(string, map[string]interface{}) {})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline exceeded, got %v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("snapshot request did not stop promptly after context cancellation")
	}
}

// TestIterateFeaturesRetriesFailedBatchFromSameTS verifies a malformed Arrow
// response does not advance the incremental cursor and the batch is retried.
func TestIterateFeaturesRetriesFailedBatchFromSameTS(t *testing.T) {
	origInterval := iterateFeaturesPollInterval
	iterateFeaturesPollInterval = 10 * time.Millisecond
	t.Cleanup(func() { iterateFeaturesPollInterval = origInterval })
	corruptIterateHits.Store(0)
	corruptRetryUsedOldTs.Store(false)

	d := newFakeTableDaoForTable(t, "corrupt-iterate")
	ctx, cancel := context.WithCancel(context.Background())
	var calls atomic.Int64
	if err := d.ScanAndIterateFeaturesWithContext(ctx, func(string, map[string]interface{}) {
		calls.Add(1)
	}); err != nil {
		cancel()
		t.Fatalf("ScanAndIterateFeaturesWithContext: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for calls.Load() <= 3 {
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("incremental callback was not invoked after retry")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	deadline = time.Now().Add(time.Second)
	for iterateGoroutineAlive() {
		if time.Now().After(deadline) {
			t.Fatal("iterate goroutine did not exit after cancel")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if corruptIterateHits.Load() < 2 {
		t.Fatalf("expected at least 2 iterate requests, got %d", corruptIterateHits.Load())
	}
	if !corruptRetryUsedOldTs.Load() {
		t.Fatal("failed iterate batch advanced ts instead of retrying from the old ts")
	}
}

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
