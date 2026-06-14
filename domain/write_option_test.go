package domain

import (
	"context"
	"errors"
	"testing"

	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/api"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/constants"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/dao"
)

// recordingDao captures calls so tests can assert which DAO method the
// FeatureView routed the write to.
type recordingDao struct {
	dao.UnimplementedFeatureViewDao
	asyncCalled  int
	directCalled int
	lastData     []map[string]interface{}
	directErr    error
}

func (r *recordingDao) WriteFeatures(data []map[string]interface{}) {
	r.asyncCalled++
	r.lastData = data
}

func (r *recordingDao) WriteFeaturesDirect(data []map[string]interface{}) error {
	r.directCalled++
	r.lastData = data
	return r.directErr
}

// Satisfy the rest of the FeatureViewDao interface via embedded Unimplemented.
var _ dao.FeatureViewDao = (*recordingDao)(nil)

func TestResolveWriteOptions(t *testing.T) {
	o := resolveWriteOptions(nil)
	if o.Direct || o.InsertMode != "" {
		t.Fatalf("zero-value WriteOptions expected, got %+v", o)
	}

	o = resolveWriteOptions([]WriteOption{WithDirect(), WithInsertMode(constants.FullRowWrite)})
	if !o.Direct {
		t.Errorf("WithDirect() should set Direct=true")
	}
	if o.InsertMode != constants.FullRowWrite {
		t.Errorf("WithInsertMode should set InsertMode, got %q", o.InsertMode)
	}
}

func TestBaseFeatureViewWriteFeatures_Default_Async(t *testing.T) {
	rd := &recordingDao{}
	fv := &BaseFeatureView{featureViewDao: rd}
	data := []map[string]interface{}{{"id": "1"}}

	if err := fv.WriteFeatures(data); err != nil {
		t.Fatalf("default write should not return error, got %v", err)
	}
	if rd.asyncCalled != 1 || rd.directCalled != 0 {
		t.Fatalf("expected async=1 direct=0, got async=%d direct=%d",
			rd.asyncCalled, rd.directCalled)
	}
}

func TestBaseFeatureViewWriteFeatures_WithDirect_RoutesDirect(t *testing.T) {
	rd := &recordingDao{}
	fv := &BaseFeatureView{featureViewDao: rd}
	data := []map[string]interface{}{{"id": "1"}}

	if err := fv.WriteFeatures(data, WithDirect()); err != nil {
		t.Fatalf("WithDirect should not return error, got %v", err)
	}
	if rd.directCalled != 1 || rd.asyncCalled != 0 {
		t.Fatalf("expected direct=1 async=0, got direct=%d async=%d",
			rd.directCalled, rd.asyncCalled)
	}
}

func TestBaseFeatureViewWriteFeatures_WithDirect_PropagatesError(t *testing.T) {
	wantErr := errors.New("backend down")
	rd := &recordingDao{directErr: wantErr}
	fv := &BaseFeatureView{featureViewDao: rd}

	err := fv.WriteFeatures([]map[string]interface{}{{"id": "1"}}, WithDirect())
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected dao error to propagate, got %v", err)
	}
}

func TestBaseFeatureViewWriteFeatures_WithDirect_RejectsPartialFieldWrite(t *testing.T) {
	rd := &recordingDao{}
	fv := &BaseFeatureView{featureViewDao: rd}

	err := fv.WriteFeatures(
		[]map[string]interface{}{{"id": "1"}},
		WithDirect(), WithInsertMode(constants.PartialFieldWrite),
	)
	if err == nil {
		t.Fatalf("WithDirect + PartialFieldWrite must error")
	}
	if rd.directCalled != 0 || rd.asyncCalled != 0 {
		t.Fatalf("no DAO call expected on validation failure, got direct=%d async=%d",
			rd.directCalled, rd.asyncCalled)
	}
}

func TestBaseFeatureViewWriteFeatures_InsertModeStampsRows(t *testing.T) {
	rd := &recordingDao{}
	fv := &BaseFeatureView{featureViewDao: rd}
	data := []map[string]interface{}{{"id": "1"}, {"id": "2"}}

	if err := fv.WriteFeatures(data, WithInsertMode(constants.FullRowWrite)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, row := range rd.lastData {
		if row["__insert_mode__"] != constants.FullRowWrite {
			t.Errorf("row missing __insert_mode__ marker: %+v", row)
		}
	}
}

func TestSequenceFeatureViewWriteFeatures_WithDirect_NotSupported(t *testing.T) {
	rd := &recordingDao{}
	fv := &SequenceFeatureView{
		FeatureView:    &api.FeatureView{},
		featureViewDao: rd,
	}

	err := fv.WriteFeatures(
		[]map[string]interface{}{{"item_id": "1"}},
		WithDirect(),
	)
	if err == nil {
		t.Fatalf("sequence FV must reject WithDirect")
	}
	if rd.directCalled != 0 {
		t.Fatalf("dao.WriteFeaturesDirect should not be invoked for sequence FV")
	}
}

// ensure context import is used to keep linters quiet across stubs that may be
// added in the future; recordingDao's embedded Unimplemented already provides
// context-aware methods, but referencing context here documents intent.
var _ = context.Background
