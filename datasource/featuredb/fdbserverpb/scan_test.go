package fdbserverpb

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
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/api"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/constants"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/datasource/featuredb"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/domain"
)

// retryTracker records how a table whose first iterate response is broken
// gets retried.
type retryTracker struct {
	hits           atomic.Int64
	retryUsedOldTs atomic.Bool
}

// iterateRetryTrackers maps a table name to the broken first iterate response
// it serves, before falling back to a valid stream.
var iterateRetryTrackers = map[string]*struct {
	writeBroken func(io.Writer)
	tracker     retryTracker
}{
	"corrupt-iterate":    {writeBroken: func(w io.Writer) { io.WriteString(w, "invalid arrow stream") }},
	"bad-schema-iterate": {writeBroken: writeBadSchemaArrowStream},
}

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

func fakeFeatureDBHandler(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/slow-snapshot/snapshots"):
		<-r.Context().Done()
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/snapshots"):
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"code":"OK","data":{"snapshot_id":"s1","ts":1}}`)
	case strings.Contains(r.URL.Path, "/bad-schema/") && strings.HasSuffix(r.URL.Path, "/scan"):
		writeBadSchemaArrowStream(w)
	case strings.HasSuffix(r.URL.Path, "/scan"):
		writeFakeArrowStream(w)
	case strings.HasSuffix(r.URL.Path, "/iterate_get_kv"):
		w.Header().Set("Next-Ts", "2")
		parts := strings.Split(r.URL.Path, "/")
		if entry, ok := iterateRetryTrackers[parts[len(parts)-2]]; ok {
			if entry.tracker.hits.Add(1) == 1 {
				entry.writeBroken(w)
				return
			}
			if r.URL.Query().Get("ts") == "1" {
				entry.tracker.retryUsedOldTs.Store(true)
			}
		}
		writeFakeArrowStream(w)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

// fakeViewFields puts the primary key in the middle and includes event time
// and partition fields to exercise the field ordering rule.
func fakeViewFields() []*api.FeatureViewFields {
	return []*api.FeatureViewFields{
		{Name: "v", Type: constants.FS_INT64},
		{Name: "pk", Type: constants.FS_STRING, IsPrimaryKey: true},
		{Name: "s", Type: constants.FS_STRING},
		{Name: "et", Type: constants.FS_INT64, IsEventTime: true},
		{Name: "p", Type: constants.FS_STRING, IsPartition: true},
		{Name: "a", Type: constants.FS_ARRAY_INT64},
		{Name: "n", Type: constants.FS_DOUBLE},
		{Name: "m", Type: constants.FS_MAP_STRING_INT32},
		{Name: "i8", Type: constants.FS_ARRAY_INT8},
		{Name: "u8", Type: constants.FS_ARRAY_UINT8},
	}
}

// fakeRowWant is the decoded form of encodeFakeRow, without the primary key.
var fakeRowWant = map[string]interface{}{
	"v":  int64(1),
	"s":  "hello",
	"et": int64(9),
	"a":  []int64{1, 2},
	"m":  map[string]int32{"k": 7},
	"i8": []int8{-1, 2},
	"u8": []uint8{1, 255},
}

// encodeFakeRow encodes a row in the FeatureDB F/1 protocol, in the field
// order v, s, et, a, n, m, i8, u8 with n being null.
func encodeFakeRow() []byte {
	var buf bytes.Buffer
	buf.WriteByte(featureDBProtocolVersionF)
	buf.WriteByte(featureDBIfNullFlagVersion1)
	w := func(v interface{}) { binary.Write(&buf, binary.LittleEndian, v) }
	// v
	buf.WriteByte(0)
	w(int64(1))
	// s
	buf.WriteByte(0)
	w(uint32(5))
	buf.WriteString("hello")
	// et
	buf.WriteByte(0)
	w(int64(9))
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

// writeBadSchemaArrowStream writes a record whose value column is a string
// instead of binary, simulating an incompatible server schema.
func writeBadSchemaArrowStream(w io.Writer) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "pk", Type: arrow.BinaryTypes.String},
		{Name: "data", Type: arrow.BinaryTypes.String},
	}, nil)
	b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer b.Release()
	b.Field(0).(*array.StringBuilder).Append("id_0")
	b.Field(1).(*array.StringBuilder).Append("not binary")
	rec := b.NewRecord()
	defer rec.Release()

	writer := ipc.NewWriter(w, ipc.WithSchema(schema))
	writer.Write(rec)
	writer.Close()
}

// iterateGoroutineAlive reports whether an iterate background goroutine is
// still present in the goroutine dump.
func iterateGoroutineAlive() bool {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	return strings.Contains(string(buf[:n]), "(*featureScanTable).iterate")
}

func waitIterateGoroutineExit(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for iterateGoroutineAlive() {
		if time.Now().After(deadline) {
			t.Fatal("iterate goroutine did not exit within 1s after cancel")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func newFakeProject() *domain.Project {
	return &domain.Project{Project: &api.Project{
		ProjectName:          "schema",
		InstanceId:           "db",
		Signature:            "sig",
		OnlineDatasourceType: constants.Datasource_Type_FeatureDB,
	}}
}

func newFakeFeatureView(project *domain.Project, table string) domain.FeatureView {
	return domain.NewFeatureView(&api.FeatureView{
		Name:   table,
		Type:   constants.Feature_View_Type_Batch,
		Fields: fakeViewFields(),
	}, project, nil)
}

func setShortPollInterval(t *testing.T) {
	t.Helper()
	origInterval := iterateFeaturesPollInterval
	iterateFeaturesPollInterval = 10 * time.Millisecond
	t.Cleanup(func() { iterateFeaturesPollInterval = origInterval })
}

// TestDecodeFeatureDBRow verifies every field type is decoded, null fields are
// omitted and truncated or unsupported data is rejected.
func TestDecodeFeatureDBRow(t *testing.T) {
	project := newFakeProject()
	scanTable := newFeatureScanTable(nil, project, newFakeFeatureView(project, "table"))
	data := encodeFakeRow()

	got, err := decodeFeatureDBRow(data, scanTable.fields, scanTable.fieldTypes)
	if err != nil {
		t.Fatalf("decodeFeatureDBRow: %v", err)
	}
	if !reflect.DeepEqual(got, fakeRowWant) {
		t.Fatalf("decodeFeatureDBRow = %#v, want %#v", got, fakeRowWant)
	}

	if _, err := decodeFeatureDBRow(data[:len(data)-2], scanTable.fields, scanTable.fieldTypes); err == nil {
		t.Fatal("expected error for truncated data")
	}
	bad := append([]byte{'X', '1'}, data[2:]...)
	if _, err := decodeFeatureDBRow(bad, scanTable.fields, scanTable.fieldTypes); err == nil {
		t.Fatal("expected error for unsupported protocol version")
	}
}

// TestFeatureScanTableMatchesFeatureViewDao verifies the field order derived
// from the feature view matches the one used by the FeatureDB DAO: rows
// encoded in our order must satisfy a filter evaluated by RowCountIds.
func TestFeatureScanTableMatchesFeatureViewDao(t *testing.T) {
	project := newFakeProject()
	featureView := newFakeFeatureView(project, "table")
	scanTable := newFeatureScanTable(nil, project, featureView)

	wantFields := []string{"v", "s", "et", "a", "n", "m", "i8", "u8"}
	if !reflect.DeepEqual(scanTable.fields, wantFields) {
		t.Fatalf("fields = %v, want %v", scanTable.fields, wantFields)
	}
	if scanTable.primaryKeyField != "pk" {
		t.Fatalf("primaryKeyField = %q, want pk", scanTable.primaryKeyField)
	}
	if scanTable.database != "db" || scanTable.schema != "schema" || scanTable.table != "table" || scanTable.signature != "sig" {
		t.Fatalf("unexpected table location: %+v", scanTable)
	}

	ids, _, err := featureView.RowCountIds(`v == 1 && s == "hello" && et == 9`)
	if err != nil {
		t.Fatalf("RowCountIds: %v", err)
	}
	if len(ids) != 3 {
		t.Fatalf("DAO decoded rows differently, got ids %v", ids)
	}
}

// TestFeatureScanTablePartitionedEventTime verifies an event time field that
// is also a partition field stays encoded, matching NewBaseFeatureView, while
// a plain partition field is skipped.
func TestFeatureScanTablePartitionedEventTime(t *testing.T) {
	project := newFakeProject()
	featureView := domain.NewFeatureView(&api.FeatureView{
		Name: "table",
		Type: constants.Feature_View_Type_Batch,
		Fields: []*api.FeatureViewFields{
			{Name: "pk", Type: constants.FS_STRING, IsPrimaryKey: true},
			{Name: "ds", Type: constants.FS_STRING, IsPartition: true},
			{Name: "et", Type: constants.FS_INT64, IsEventTime: true, IsPartition: true},
			{Name: "v", Type: constants.FS_INT64},
		},
	}, project, nil)
	scanTable := newFeatureScanTable(nil, project, featureView)

	if want := []string{"et", "v"}; !reflect.DeepEqual(scanTable.fields, want) {
		t.Fatalf("fields = %v, want %v", scanTable.fields, want)
	}
	wantTypes := map[string]constants.FSType{"pk": constants.FS_STRING, "v": constants.FS_INT64}
	if !reflect.DeepEqual(scanTable.fieldTypes, wantTypes) {
		t.Fatalf("fieldTypes = %v, want %v", scanTable.fieldTypes, wantTypes)
	}
}

// TestScanAndIterateFeatures verifies the full scan calls fn synchronously for
// every row with all features and the primary key, the incremental goroutine
// keeps calling fn, and it exits within 1s after ctx is canceled.
func TestScanAndIterateFeatures(t *testing.T) {
	setShortPollInterval(t)
	project := newFakeProject()
	featureView := newFakeFeatureView(project, "table")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mu sync.Mutex
	var calls []map[string]interface{}
	err := ScanAndIterateFeatures(ctx, project, featureView, func(id string, properties map[string]interface{}) {
		mu.Lock()
		calls = append(calls, properties)
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("ScanAndIterateFeatures: %v", err)
	}

	mu.Lock()
	scanned := len(calls)
	first := calls[0]
	mu.Unlock()
	if scanned < 3 {
		t.Fatalf("expected fn to be called for 3 scanned rows before return, got %d", scanned)
	}
	want := map[string]interface{}{"pk": "id_0"}
	for k, v := range fakeRowWant {
		want[k] = v
	}
	if !reflect.DeepEqual(first, want) {
		t.Fatalf("first row = %#v, want %#v", first, want)
	}

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
	waitIterateGoroutineExit(t)

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

// TestScanAndIterateFeaturesRejectsInvalidArgs verifies invalid arguments,
// including contexts that can never be canceled, fail fast without calling fn
// or starting the iterate goroutine.
func TestScanAndIterateFeaturesRejectsInvalidArgs(t *testing.T) {
	project := newFakeProject()
	featureView := newFakeFeatureView(project, "table")
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	live, stop := context.WithCancel(context.Background())
	defer stop()
	noop := func(string, map[string]interface{}) {}

	sequenceView := &fakeFeatureView{FeatureView: featureView, typ: constants.Feature_View_Type_Sequence}
	dynamicEmbeddingView := &fakeFeatureView{FeatureView: featureView, typ: constants.Feature_View_Type_DynamicEmbedding}

	cases := []struct {
		name        string
		ctx         context.Context
		project     *domain.Project
		featureView domain.FeatureView
		fn          func(string, map[string]interface{})
	}{
		{"nil ctx", nil, project, featureView, noop},
		{"background ctx", context.Background(), project, featureView, noop},
		{"canceled ctx", canceled, project, featureView, noop},
		{"nil fn", live, project, featureView, nil},
		{"nil project", live, nil, featureView, noop},
		{"nil feature view", live, project, nil, noop},
		{"sequence feature view", live, project, sequenceView, noop},
		{"dynamic embedding feature view", live, project, dynamicEmbeddingView, noop},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			called := false
			fn := c.fn
			if fn != nil {
				fn = func(string, map[string]interface{}) { called = true }
			}
			if err := ScanAndIterateFeatures(c.ctx, c.project, c.featureView, fn); err == nil {
				t.Fatal("expected error")
			}
			if called {
				t.Fatal("fn must not be called")
			}
			if iterateGoroutineAlive() {
				t.Fatal("iterate goroutine must not start")
			}
		})
	}
}

// fakeFeatureView overrides the type of a real feature view.
type fakeFeatureView struct {
	domain.FeatureView
	typ string
}

func (f *fakeFeatureView) GetType() string { return f.typ }

// TestScanAndIterateFeaturesCancelsSnapshot verifies cancellation interrupts an
// in-flight snapshot request before scan or iteration starts.
func TestScanAndIterateFeaturesCancelsSnapshot(t *testing.T) {
	project := newFakeProject()
	featureView := newFakeFeatureView(project, "slow-snapshot")
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := ScanAndIterateFeatures(ctx, project, featureView, func(string, map[string]interface{}) {})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline exceeded, got %v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("snapshot request did not stop promptly after context cancellation")
	}
}

// TestScanAndIterateFeaturesRejectsBadSchema verifies a scan response whose
// value column is not binary fails with an error instead of panicking.
func TestScanAndIterateFeaturesRejectsBadSchema(t *testing.T) {
	project := newFakeProject()
	featureView := newFakeFeatureView(project, "bad-schema")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	called := false
	err := ScanAndIterateFeatures(ctx, project, featureView, func(string, map[string]interface{}) { called = true })
	if err == nil || !strings.Contains(err.Error(), "want binary") {
		t.Fatalf("expected schema error, got %v", err)
	}
	if called {
		t.Fatal("fn must not be called for a bad schema")
	}
	if iterateGoroutineAlive() {
		t.Fatal("iterate goroutine must not start when the scan fails")
	}
}

// TestScanAndIterateFeaturesRetriesFailedBatchFromSameTS verifies a malformed
// Arrow response or an unexpected schema does not advance the incremental
// cursor or panic, and the batch is retried.
func TestScanAndIterateFeaturesRetriesFailedBatchFromSameTS(t *testing.T) {
	setShortPollInterval(t)
	for _, table := range []string{"corrupt-iterate", "bad-schema-iterate"} {
		t.Run(table, func(t *testing.T) {
			tracker := &iterateRetryTrackers[table].tracker
			tracker.hits.Store(0)
			tracker.retryUsedOldTs.Store(false)

			project := newFakeProject()
			featureView := newFakeFeatureView(project, table)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var calls atomic.Int64
			if err := ScanAndIterateFeatures(ctx, project, featureView, func(string, map[string]interface{}) {
				calls.Add(1)
			}); err != nil {
				t.Fatalf("ScanAndIterateFeatures: %v", err)
			}

			deadline := time.Now().Add(2 * time.Second)
			for calls.Load() <= 3 {
				if time.Now().After(deadline) {
					t.Fatal("incremental callback was not invoked after retry")
				}
				time.Sleep(5 * time.Millisecond)
			}
			cancel()
			waitIterateGoroutineExit(t)

			if tracker.hits.Load() < 2 {
				t.Fatalf("expected at least 2 iterate requests, got %d", tracker.hits.Load())
			}
			if !tracker.retryUsedOldTs.Load() {
				t.Fatal("failed iterate batch advanced ts instead of retrying from the old ts")
			}
		})
	}
}
