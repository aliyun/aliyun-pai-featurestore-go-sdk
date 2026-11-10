package fdbserverpb

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/arrow/array"
	"github.com/aliyun/aliyun-odps-go-sdk/arrow/ipc"
	"github.com/aliyun/aliyun-odps-go-sdk/arrow/memory"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/constants"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/datasource/featuredb"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/domain"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/utils"
)

const (
	featureDBProtocolVersionF   = byte('F')
	featureDBIfNullFlagVersion1 = byte('1')
)

// iterateFeaturesPollInterval is the delay between two iterate_get_kv polls in
// ScanAndIterateFeatures. It is a variable so tests can shorten it.
var iterateFeaturesPollInterval = 1 * time.Second

// ScanAndIterateFeatures scans all rows of a FeatureDB backed feature view and
// calls fn with the primary key and the selected feature values of every row.
// The full scan runs synchronously: fn has been called for every scanned row
// when it returns nil. It then starts a background goroutine that polls
// incremental data every second and calls fn for each returned row.
//
// selectFields lists the feature names to return. []string{"*"} returns all
// features, and an unknown feature name is rejected. Unselected fields are
// skipped while decoding without allocating their values; the server still
// returns every field.
//
// Lifecycle and concurrency contract:
//   - ctx must be cancelable. Canceling ctx is the only way to stop the
//     background goroutine; closing the feature view or the periodic project
//     refresh does not stop it.
//   - After ScanAndIterateFeatures returns, fn keeps being called from the
//     background goroutine. Calls are sequential, but any state shared by fn
//     with other goroutines must be synchronized by the caller.
//   - A panic in fn is not recovered and crashes the process.
//   - The database, table, signature and field definitions are captured at
//     call time and are not refreshed afterwards.
//   - properties always holds the primary key under the primary key field
//     name, null fields are omitted, and a new map is allocated for every
//     call, so fn may retain it.
//   - A failed incremental batch is retried from the same position, so rows of
//     that batch may not be delivered until a retry succeeds.
func ScanAndIterateFeatures(ctx context.Context, project *domain.Project, featureView domain.FeatureView, selectFields []string, fn func(id string, properties map[string]interface{})) error {
	if ctx == nil || ctx.Done() == nil {
		return errors.New("ctx must be cancelable")
	}
	if fn == nil {
		return errors.New("fn must not be nil")
	}
	if project == nil || project.Project == nil || featureView == nil {
		return errors.New("project and featureView must not be nil")
	}
	switch featureView.GetType() {
	case constants.Feature_View_Type_Sequence, constants.Feature_View_Type_DynamicEmbedding:
		return fmt.Errorf("feature view type %s does not support ScanAndIterateFeatures", featureView.GetType())
	}
	fdbClient, err := featuredb.GetFeatureDBClient()
	if err != nil {
		return err
	}

	t := newFeatureScanTable(fdbClient, project, featureView)
	if err := t.setSelectFields(selectFields); err != nil {
		return err
	}
	snapshotId, ts, err := t.createSnapshot(ctx)
	if err != nil {
		return err
	}
	if err := t.scan(ctx, snapshotId, fn); err != nil {
		return err
	}

	go t.iterate(ctx, ts, iterateFeaturesPollInterval, fn)

	return nil
}

// featureScanTable holds everything needed to read one FeatureDB table.
type featureScanTable struct {
	client          *featuredb.FeatureDBClient
	database        string
	schema          string
	table           string
	signature       string
	primaryKeyField string
	// fields lists the encoded fields in row order, matching the fields that
	// NewBaseFeatureView hands to the FeatureDB DAO.
	fields     []string
	fieldTypes map[string]constants.FSType
	// selected holds the fields to decode, or nil to decode all fields.
	selected map[string]bool
}

func newFeatureScanTable(client *featuredb.FeatureDBClient, project *domain.Project, featureView domain.FeatureView) *featureScanTable {
	t := &featureScanTable{
		client:     client,
		database:   project.InstanceId,
		schema:     project.ProjectName,
		table:      featureView.GetName(),
		signature:  project.Signature,
		fieldTypes: make(map[string]constants.FSType),
	}
	// Keep the same rules as NewBaseFeatureView, because the F/1 row encoding
	// depends on the field order: an event time field is always encoded, even
	// when it is also a partition field, while field types skip partitions.
	for _, field := range featureView.GetFields() {
		if !field.IsPartition {
			t.fieldTypes[field.Name] = field.Type
		}
		if field.IsEventTime {
			t.fields = append(t.fields, field.Name)
		} else if field.IsPartition {
			continue
		} else if field.IsPrimaryKey {
			t.primaryKeyField = field.Name
		} else {
			t.fields = append(t.fields, field.Name)
		}
	}
	return t
}

// setSelectFields validates selectFields against the encoded fields. "*"
// selects all fields, and the primary key is always returned so it does not
// need to be selected.
func (t *featureScanTable) setSelectFields(selectFields []string) error {
	selected := make(map[string]bool, len(selectFields))
	selectAll := false
	for _, name := range selectFields {
		if name == "*" {
			selectAll = true
			continue
		}
		if name == t.primaryKeyField {
			continue
		}
		if !containsField(t.fields, name) {
			return fmt.Errorf("feature name :%s not found in the featureview fields", name)
		}
		selected[name] = true
	}
	if selectAll {
		selected = nil
	}
	t.selected = selected
	return nil
}

func containsField(fields []string, name string) bool {
	for _, field := range fields {
		if field == name {
			return true
		}
	}
	return false
}

func (t *featureScanTable) newRequest(ctx context.Context, method, url string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(nil))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", t.client.Token)
	req.Header.Set("Auth", t.signature)
	return req, nil
}

func (t *featureScanTable) createSnapshot(ctx context.Context) (string, int64, error) {
	if err := ctx.Err(); err != nil {
		return "", 0, err
	}
	req, err := t.newRequest(ctx, "POST", fmt.Sprintf("%s/api/v1/tables/%s/%s/%s/snapshots",
		t.client.GetNormalAddress(), t.database, t.schema, t.table))
	if err != nil {
		return "", 0, err
	}
	response, err := t.client.Client.Do(req)
	if err != nil {
		return "", 0, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		return "", 0, fmt.Errorf("status code: %d, response body: %s", response.StatusCode, string(body))
	}
	responseBody := struct {
		RequestId string         `json:"request_id,omitempty"`
		Code      string         `json:"code"`
		Message   string         `json:"message,omitempty"`
		Data      map[string]any `json:"data,omitempty"`
	}{}
	decoder := json.NewDecoder(response.Body)
	decoder.UseNumber()
	if err := decoder.Decode(&responseBody); err != nil {
		return "", 0, err
	}
	snapshotId, ok := responseBody.Data["snapshot_id"].(string)
	if !ok || snapshotId == "" {
		return "", 0, errors.New("snapshot_id is missing from FeatureDB response")
	}
	return snapshotId, utils.ToInt64(responseBody.Data["ts"], 0), nil
}

// scan reads the snapshot and calls fn for every row. It fails on the first
// request, Arrow or decode error.
func (t *featureScanTable) scan(ctx context.Context, snapshotId string, fn func(id string, properties map[string]interface{})) error {
	req, err := t.newRequest(ctx, "GET", fmt.Sprintf("%s/api/v1/tables/%s/%s/%s/snapshots/%s/scan",
		t.client.GetCurrentAddress(false), t.database, t.schema, t.table, snapshotId))
	if err != nil {
		return err
	}
	response, err := t.client.Client.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		return fmt.Errorf("status code: %d, response body: %s", response.StatusCode, string(body))
	}

	reader, err := ipc.NewReader(response.Body, ipc.WithAllocator(memory.NewGoAllocator()))
	if err != nil {
		return err
	}
	defer reader.Release()
	for reader.Next() {
		record := reader.Record()
		pks, values, err := recordColumns(record)
		if err != nil {
			return err
		}
		for i := 0; i < int(record.NumRows()); i++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			dataBytes := values.Value(i)
			if len(dataBytes) < 2 {
				continue
			}
			id := pks.Value(i)
			properties, err := decodeFeatureDBRow(dataBytes, t.fields, t.fieldTypes, t.selected)
			if err != nil {
				return fmt.Errorf("FeatureDB decode key %s error: %w", id, err)
			}
			properties[t.primaryKeyField] = id
			fn(id, properties)
		}
	}
	return reader.Err()
}

// iterate polls iterate_get_kv every interval starting from ts and calls fn
// for each returned row. A batch is fully read and decoded before any
// callback, and ts only advances after the batch succeeds, so a failed batch
// is retried from the same ts without skipping or partially delivering rows.
// It returns once ctx is done.
func (t *featureScanTable) iterate(ctx context.Context, ts int64, interval time.Duration, fn func(id string, properties map[string]interface{})) {
	alloc := memory.NewGoAllocator()
	type featureRow struct {
		id         string
		properties map[string]interface{}
	}
	for {
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}

		req, err := t.newRequest(ctx, "GET", fmt.Sprintf("%s/api/v1/tables/%s/%s/%s/iterate_get_kv?ts=%d",
			t.client.GetNormalAddress(), t.database, t.schema, t.table, ts))
		if err != nil {
			continue
		}
		response, err := t.client.Client.Do(req)
		if err != nil {
			continue
		}
		if response.StatusCode != http.StatusOK {
			response.Body.Close()
			continue
		}
		nextTs := utils.ToInt64(response.Header.Get("Next-Ts"), 0)
		if nextTs == 0 {
			response.Body.Close()
			continue
		}
		reader, err := ipc.NewReader(response.Body, ipc.WithAllocator(alloc))
		if err != nil {
			response.Body.Close()
			log.Printf("FeatureDB iterate response decode failed at ts %d: %v", ts, err)
			continue
		}

		var rows []featureRow
		var batchErr error
		for batchErr == nil && reader.Next() {
			record := reader.Record()
			pks, values, err := recordColumns(record)
			if err != nil {
				batchErr = err
				break
			}
			for i := 0; i < int(record.NumRows()); i++ {
				if err := ctx.Err(); err != nil {
					batchErr = err
					break
				}
				dataBytes := values.Value(i)
				if len(dataBytes) < 2 {
					continue
				}
				id := pks.Value(i)
				properties, err := decodeFeatureDBRow(dataBytes, t.fields, t.fieldTypes, t.selected)
				if err != nil {
					batchErr = fmt.Errorf("FeatureDB decode key %s error: %w", id, err)
					break
				}
				properties[t.primaryKeyField] = id
				rows = append(rows, featureRow{id: id, properties: properties})
			}
		}
		if batchErr == nil {
			batchErr = reader.Err()
		}
		reader.Release()
		response.Body.Close()
		if batchErr != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("FeatureDB iterate batch failed at ts %d: %v", ts, batchErr)
			continue
		}
		for _, row := range rows {
			if ctx.Err() != nil {
				return
			}
			fn(row.id, row.properties)
		}
		ts = nextTs
	}
}

// recordColumns returns the primary key and value columns of a FeatureDB scan
// or iterate record, or an error when the schema is not the expected
// (string, binary) layout, so an incompatible response never panics.
func recordColumns(record array.Record) (*array.String, *array.Binary, error) {
	if record.NumCols() < 2 {
		return nil, nil, fmt.Errorf("FeatureDB record has %d columns, want at least 2", record.NumCols())
	}
	pks, ok := record.Column(0).(*array.String)
	if !ok {
		return nil, nil, fmt.Errorf("FeatureDB record column 0 is %s, want string", record.Column(0).DataType())
	}
	values, ok := record.Column(1).(*array.Binary)
	if !ok {
		return nil, nil, fmt.Errorf("FeatureDB record column 1 is %s, want binary", record.Column(1).DataType())
	}
	return pks, values, nil
}

// decodeFeatureDBRow decodes the fields of a FeatureDB F/1 encoded row, using
// the same value types as GetOnlineFeatures. Null fields are omitted. When
// selected is not nil, only the selected fields are decoded: the others are
// skipped without allocating, and decoding stops after the last selected
// field.
func decodeFeatureDBRow(dataBytes []byte, fields []string, fieldTypes map[string]constants.FSType, selected map[string]bool) (map[string]interface{}, error) {
	dataCursor := utils.NewByteCursor(dataBytes)
	protocolVersion := dataCursor.ReadUint8()
	ifNullFlagVersion := dataCursor.ReadUint8()
	if protocolVersion != featureDBProtocolVersionF || ifNullFlagVersion != featureDBIfNullFlagVersion1 {
		return nil, fmt.Errorf("protocalVersion %v or ifNullFlagVersion %d is not supported", protocolVersion, ifNullFlagVersion)
	}

	capacity := len(fields) + 1
	remaining := -1
	if selected != nil {
		capacity = len(selected) + 1
		remaining = len(selected)
	}
	properties := make(map[string]interface{}, capacity)
	for _, field := range fields {
		if remaining == 0 {
			break
		}
		isNull, ok := dataCursor.TryReadUint8()
		if !ok {
			// EOF
			break
		}
		if selected != nil {
			if !selected[field] {
				if isNull != 1 {
					skipFeatureDBValue(dataCursor, fieldTypes[field])
					if dataCursor.Err != nil {
						return nil, fmt.Errorf("field %s: %w", field, dataCursor.Err)
					}
				}
				continue
			}
			remaining--
		}
		if isNull == 1 {
			continue
		}
		switch fieldTypes[field] {
		case constants.FS_DOUBLE:
			properties[field] = dataCursor.ReadFloat64()
		case constants.FS_FLOAT:
			properties[field] = dataCursor.ReadFloat32()
		case constants.FS_INT64:
			properties[field] = dataCursor.ReadInt64()
		case constants.FS_INT32:
			properties[field] = dataCursor.ReadInt32()
		case constants.FS_BOOLEAN:
			properties[field] = dataCursor.ReadBool()
		case constants.FS_STRING:
			properties[field] = dataCursor.ReadString()
		case constants.FS_TIMESTAMP:
			properties[field] = time.UnixMilli(dataCursor.ReadInt64())
		case constants.FS_ARRAY_INT32:
			properties[field] = dataCursor.ReadInt32Slice(dataCursor.ReadUint32())
		case constants.FS_ARRAY_INT64:
			properties[field] = dataCursor.ReadInt64Slice(dataCursor.ReadUint32())
		case constants.FS_ARRAY_FLOAT:
			properties[field] = dataCursor.ReadFloat32Slice(dataCursor.ReadUint32())
		case constants.FS_ARRAY_DOUBLE:
			properties[field] = dataCursor.ReadFloat64Slice(dataCursor.ReadUint32())
		case constants.FS_ARRAY_STRING:
			properties[field] = dataCursor.ReadStringArray(dataCursor.ReadUint32())
		case constants.FS_ARRAY_INT8:
			raw := dataCursor.ReadBytes(int(dataCursor.ReadUint32()))
			values := make([]int8, len(raw))
			for i, value := range raw {
				values[i] = int8(value)
			}
			properties[field] = values
		case constants.FS_ARRAY_UINT8:
			raw := dataCursor.ReadBytes(int(dataCursor.ReadUint32()))
			values := make([]uint8, len(raw))
			copy(values, raw)
			properties[field] = values
		case constants.FS_ARRAY_ARRAY_FLOAT:
			outerLength := dataCursor.ReadUint32()
			if outerLength == 0 {
				properties[field] = [][]float32{}
			} else {
				totalElements := dataCursor.ReadUint32()
				if totalElements == 0 {
					arr := make([][]float32, outerLength)
					for k := uint32(0); k < outerLength; k++ {
						arr[k] = []float32{}
					}
					properties[field] = arr
				} else {
					innerArrayLens := dataCursor.ReadUint32Slice(outerLength)
					innerValidElements := dataCursor.ReadFloat32Slice(totalElements)
					result := make([][]float32, outerLength)
					innerIndex := 0
					for outerIdx, innerLength := range innerArrayLens {
						if innerIndex+int(innerLength) > len(innerValidElements) {
							return nil, fmt.Errorf("field %s: inner array length out of range", field)
						}
						result[outerIdx] = innerValidElements[innerIndex : innerIndex+int(innerLength)]
						innerIndex += int(innerLength)
					}
					properties[field] = result
				}
			}
		case constants.FS_MAP_INT32_INT32:
			properties[field] = readMap(dataCursor, dataCursor.ReadInt32Slice, dataCursor.ReadInt32Slice)
		case constants.FS_MAP_INT32_INT64:
			properties[field] = readMap(dataCursor, dataCursor.ReadInt32Slice, dataCursor.ReadInt64Slice)
		case constants.FS_MAP_INT32_FLOAT:
			properties[field] = readMap(dataCursor, dataCursor.ReadInt32Slice, dataCursor.ReadFloat32Slice)
		case constants.FS_MAP_INT32_DOUBLE:
			properties[field] = readMap(dataCursor, dataCursor.ReadInt32Slice, dataCursor.ReadFloat64Slice)
		case constants.FS_MAP_INT32_STRING:
			properties[field] = readMap(dataCursor, dataCursor.ReadInt32Slice, dataCursor.ReadStringArray)
		case constants.FS_MAP_INT64_INT32:
			properties[field] = readMap(dataCursor, dataCursor.ReadInt64Slice, dataCursor.ReadInt32Slice)
		case constants.FS_MAP_INT64_INT64:
			properties[field] = readMap(dataCursor, dataCursor.ReadInt64Slice, dataCursor.ReadInt64Slice)
		case constants.FS_MAP_INT64_FLOAT:
			properties[field] = readMap(dataCursor, dataCursor.ReadInt64Slice, dataCursor.ReadFloat32Slice)
		case constants.FS_MAP_INT64_DOUBLE:
			properties[field] = readMap(dataCursor, dataCursor.ReadInt64Slice, dataCursor.ReadFloat64Slice)
		case constants.FS_MAP_INT64_STRING:
			properties[field] = readMap(dataCursor, dataCursor.ReadInt64Slice, dataCursor.ReadStringArray)
		case constants.FS_MAP_STRING_INT32:
			properties[field] = readMap(dataCursor, dataCursor.ReadStringArray, dataCursor.ReadInt32Slice)
		case constants.FS_MAP_STRING_INT64:
			properties[field] = readMap(dataCursor, dataCursor.ReadStringArray, dataCursor.ReadInt64Slice)
		case constants.FS_MAP_STRING_FLOAT:
			properties[field] = readMap(dataCursor, dataCursor.ReadStringArray, dataCursor.ReadFloat32Slice)
		case constants.FS_MAP_STRING_DOUBLE:
			properties[field] = readMap(dataCursor, dataCursor.ReadStringArray, dataCursor.ReadFloat64Slice)
		case constants.FS_MAP_STRING_STRING:
			properties[field] = readMap(dataCursor, dataCursor.ReadStringArray, dataCursor.ReadStringArray)
		default:
			properties[field] = dataCursor.ReadStringArray(dataCursor.ReadUint32())
		}
		if dataCursor.Err != nil {
			return nil, fmt.Errorf("field %s: %w", field, dataCursor.Err)
		}
	}
	return properties, nil
}

// skipFeatureDBValue moves the cursor past one non-null value of fsType in
// the layout read by decodeFeatureDBRow, without allocating it.
func skipFeatureDBValue(dataCursor *utils.ByteCursor, fsType constants.FSType) {
	switch fsType {
	case constants.FS_DOUBLE, constants.FS_INT64, constants.FS_TIMESTAMP:
		dataCursor.Skip(8)
	case constants.FS_FLOAT, constants.FS_INT32:
		dataCursor.Skip(4)
	case constants.FS_BOOLEAN:
		dataCursor.Skip(1)
	case constants.FS_STRING, constants.FS_ARRAY_INT8, constants.FS_ARRAY_UINT8:
		dataCursor.Skip(int(dataCursor.ReadUint32()))
	case constants.FS_ARRAY_INT32, constants.FS_ARRAY_FLOAT:
		dataCursor.Skip(int(dataCursor.ReadUint32()) * 4)
	case constants.FS_ARRAY_INT64, constants.FS_ARRAY_DOUBLE:
		dataCursor.Skip(int(dataCursor.ReadUint32()) * 8)
	case constants.FS_ARRAY_ARRAY_FLOAT:
		outerLength := dataCursor.ReadUint32()
		if outerLength == 0 {
			return
		}
		totalElements := dataCursor.ReadUint32()
		if totalElements == 0 {
			return
		}
		dataCursor.Skip(int(outerLength) * 4)
		dataCursor.Skip(int(totalElements) * 4)
	default:
		if sizes, ok := mapElementSizes[fsType]; ok {
			length := dataCursor.ReadUint32()
			if length == 0 {
				return
			}
			skipElements(dataCursor, length, sizes[0])
			skipElements(dataCursor, length, sizes[1])
			return
		}
		dataCursor.SkipStringArray(dataCursor.ReadUint32())
	}
}

// mapElementSizes holds the key and value element sizes in bytes of every map
// type, where 0 stands for a string array.
var mapElementSizes = map[constants.FSType][2]int{
	constants.FS_MAP_INT32_INT32:   {4, 4},
	constants.FS_MAP_INT32_INT64:   {4, 8},
	constants.FS_MAP_INT32_FLOAT:   {4, 4},
	constants.FS_MAP_INT32_DOUBLE:  {4, 8},
	constants.FS_MAP_INT32_STRING:  {4, 0},
	constants.FS_MAP_INT64_INT32:   {8, 4},
	constants.FS_MAP_INT64_INT64:   {8, 8},
	constants.FS_MAP_INT64_FLOAT:   {8, 4},
	constants.FS_MAP_INT64_DOUBLE:  {8, 8},
	constants.FS_MAP_INT64_STRING:  {8, 0},
	constants.FS_MAP_STRING_INT32:  {0, 4},
	constants.FS_MAP_STRING_INT64:  {0, 8},
	constants.FS_MAP_STRING_FLOAT:  {0, 4},
	constants.FS_MAP_STRING_DOUBLE: {0, 8},
	constants.FS_MAP_STRING_STRING: {0, 0},
}

// skipElements skips count fixed size elements, or a string array of count
// elements when size is 0.
func skipElements(dataCursor *utils.ByteCursor, count uint32, size int) {
	if size == 0 {
		dataCursor.SkipStringArray(count)
		return
	}
	dataCursor.Skip(int(count) * size)
}

// readMap reads a length followed by the key array and the value array.
func readMap[K comparable, V any](dataCursor *utils.ByteCursor, readKeys func(uint32) []K, readValues func(uint32) []V) map[K]V {
	length := dataCursor.ReadUint32()
	m := make(map[K]V, length)
	if length == 0 {
		return m
	}
	keys := readKeys(length)
	values := readValues(length)
	for k := 0; k < len(keys) && k < len(values); k++ {
		m[keys[k]] = values[k]
	}
	return m
}
