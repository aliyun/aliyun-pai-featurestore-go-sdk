package dao

import (
	"bytes"
	"context"
	"encoding/binary"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/constants"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/datasource/featuredb"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/datasource/featuredb/fdbserverfb"
	flatbuffers "github.com/google/flatbuffers/go"
)

// These tests drive the real GetFeaturesWithContext decode path against an
// in-process httptest server that returns a hand-built flatbuffers RecordBlock,
// so the FS_BINARY branch is exercised end to end without a live FeatureDB.
//
// They lock BLK-1: the FS_BINARY value returned to the caller must be an
// independent copy, not an alias into the block buffer. An alias would (a) pin
// the whole block while any single value is retained and (b) carry spare
// capacity reaching the end of the block, so a caller append silently corrupts
// neighbouring fields/rows.

var (
	binarySrvOnce sync.Once
	binarySrv     *httptest.Server
	binaryRespMu  sync.Mutex
	binaryResp    []byte
)

// binaryTestClient lazily starts one shared httptest server and points the
// process-wide FeatureDB client singleton at it. The server is shared so the
// tests stay stable under -count=N (the singleton is initialized only once).
func binaryTestClient(t *testing.T) *featuredb.FeatureDBClient {
	t.Helper()
	binarySrvOnce.Do(func() {
		binarySrv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			binaryRespMu.Lock()
			payload := binaryResp
			binaryRespMu.Unlock()
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(payload)
		}))
		// Empty vpcAddress => no background health-check goroutine is started.
		featuredb.InitFeatureDBClient(binarySrv.URL, "test-token", "", true)
	})
	client, err := featuredb.GetFeatureDBClient()
	if err != nil {
		t.Fatalf("GetFeatureDBClient: %v", err)
	}
	return client
}

func setBinaryResp(payload []byte) {
	binaryRespMu.Lock()
	binaryResp = payload
	binaryRespMu.Unlock()
}

// newBinaryDao builds a DAO whose row layout is [bin FS_BINARY, tail FS_BINARY].
// Tests select only "bin"; "tail" is decoded-but-skipped, which keeps bytes after
// the binary value inside the same row vector so the alias hazard is observable.
func newBinaryDao(client *featuredb.FeatureDBClient) *FeatureViewFeatureDBDao {
	return &FeatureViewFeatureDBDao{
		featureDBClient: client,
		database:        "db",
		schema:          "sch",
		table:           "tbl",
		fieldTypeMap: map[string]constants.FSType{
			"bin":  constants.FS_BINARY,
			"tail": constants.FS_BINARY,
		},
		fields:          []string{"bin", "tail"},
		signature:       "sig",
		primaryKeyField: "id",
		writeData:       make([]map[string]interface{}, 0),
		batchSize:       20,
		stopChan:        make(chan struct{}),
		done:            make(chan struct{}),
	}
}

// appendBinaryField appends one non-null FS_BINARY field: isNull=0, uint32 LE
// length, payload.
func appendBinaryField(row, payload []byte) []byte {
	row = append(row, 0)
	var l [4]byte
	binary.LittleEndian.PutUint32(l[:], uint32(len(payload)))
	row = append(row, l[:]...)
	return append(row, payload...)
}

// encodeBinaryRow builds F/1 wire bytes for a row with a single binary field.
func encodeBinaryRow(payload []byte) []byte {
	row := []byte{FeatureDB_Protocal_Version_F, FeatureDB_IfNull_Flag_Version_1}
	return appendBinaryField(row, payload)
}

// encodeBinaryRowWithTail builds a row [bin, tail] so the bin value is followed
// by more bytes within the same row vector.
func encodeBinaryRowWithTail(binPayload, tailPayload []byte) []byte {
	return appendBinaryField(encodeBinaryRow(binPayload), tailPayload)
}

// encodeForgedBinaryRow builds a row whose binary length prefix is declaredLen
// but which carries no payload, to exercise the oversized-length guard.
func encodeForgedBinaryRow(declaredLen uint32) []byte {
	row := []byte{FeatureDB_Protocal_Version_F, FeatureDB_IfNull_Flag_Version_1, 0}
	var l [4]byte
	binary.LittleEndian.PutUint32(l[:], declaredLen)
	return append(row, l[:]...)
}

// buildRecordBlock packs rows into a finished flatbuffers RecordBlock buffer.
func buildRecordBlock(rows [][]byte) []byte {
	b := flatbuffers.NewBuilder(1024)
	cols := make([]flatbuffers.UOffsetT, len(rows))
	for i, row := range rows {
		fdbserverfb.UInt8ValueColumnStartValueVector(b, len(row))
		for j := len(row) - 1; j >= 0; j-- {
			b.PrependByte(row[j])
		}
		vec := b.EndVector(len(row))
		fdbserverfb.UInt8ValueColumnStart(b)
		fdbserverfb.UInt8ValueColumnAddValue(b, vec)
		cols[i] = fdbserverfb.UInt8ValueColumnEnd(b)
	}
	fdbserverfb.RecordBlockStartValuesVector(b, len(rows))
	for i := len(rows) - 1; i >= 0; i-- {
		b.PrependUOffsetT(cols[i])
	}
	values := b.EndVector(len(rows))
	fdbserverfb.RecordBlockStart(b)
	fdbserverfb.RecordBlockAddValues(b, values)
	rb := fdbserverfb.RecordBlockEnd(b)
	fdbserverfb.FinishRecordBlockBuffer(b, rb)
	return b.FinishedBytes()
}

// frameBlock prefixes a block with its uint32 LE length, matching deserialize().
func frameBlock(block []byte) []byte {
	out := make([]byte, 4+len(block))
	binary.LittleEndian.PutUint32(out[:4], uint32(len(block)))
	copy(out[4:], block)
	return out
}

func TestGetFeaturesBinaryIsIndependentCopy(t *testing.T) {
	client := binaryTestClient(t)
	d := newBinaryDao(client)

	t.Run("copy_not_alias", func(t *testing.T) {
		p0, p1 := []byte{1, 2, 3, 4}, []byte{9, 8, 7, 6}
		tail := []byte{0xAA, 0xBB}
		setBinaryResp(frameBlock(buildRecordBlock([][]byte{
			encodeBinaryRowWithTail(p0, tail),
			encodeBinaryRowWithTail(p1, tail),
		})))

		res, err := d.GetFeaturesWithContext(context.Background(), []interface{}{"k1", "k2"}, []string{"bin"}, 0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(res) != 2 {
			t.Fatalf("expected 2 rows, got %d", len(res))
		}
		for i, want := range [][]byte{p0, p1} {
			got, ok := res[i]["bin"].([]byte)
			if !ok {
				t.Fatalf("row %d: bin is %T, want []byte", i, res[i]["bin"])
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("row %d: bin = %v, want %v", i, got, want)
			}
			// The fix copies into a right-sized slice: cap == len. An alias
			// into the row/block buffer has cap > len (spare capacity reaching
			// past the value), which is exactly the BLK-1 hazard.
			if cap(got) != len(got) {
				t.Fatalf("row %d: cap(bin)=%d != len(bin)=%d — value aliases the block buffer", i, cap(got), len(got))
			}
		}
		// Appending to one row's value must not disturb the other row's value.
		b0 := res[0]["bin"].([]byte)
		b0 = append(b0, 0xFF, 0xFF, 0xFF, 0xFF)
		_ = b0
		if !bytes.Equal(res[1]["bin"].([]byte), p1) {
			t.Fatalf("row 1 bin corrupted after appending to row 0: %v", res[1]["bin"])
		}
	})

	t.Run("zero_length", func(t *testing.T) {
		setBinaryResp(frameBlock(buildRecordBlock([][]byte{
			encodeBinaryRowWithTail([]byte{}, []byte{0xAA}),
		})))
		res, err := d.GetFeaturesWithContext(context.Background(), []interface{}{"k1"}, []string{"bin"}, 0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(res) != 1 {
			t.Fatalf("expected 1 row, got %d", len(res))
		}
		got, ok := res[0]["bin"].([]byte)
		if !ok || len(got) != 0 {
			t.Fatalf("expected empty []byte, got %T %v", res[0]["bin"], res[0]["bin"])
		}
	})

	t.Run("forged_oversized_length_rejected", func(t *testing.T) {
		// Declared length ~4GiB but the row carries no payload. The decoder must
		// reject via the cursor bounds check BEFORE allocating, returning an
		// error instead of OOM-ing the process. This guards against sizing
		// make() directly from the untrusted length prefix (the NB-1/NB-2 pitfall).
		setBinaryResp(frameBlock(buildRecordBlock([][]byte{
			encodeForgedBinaryRow(0xFFFFFFF0),
		})))
		res, err := d.GetFeaturesWithContext(context.Background(), []interface{}{"k1"}, []string{"bin"}, 0)
		if err == nil {
			t.Fatalf("expected error for oversized binary length, got nil (res=%v)", res)
		}
	})
}
