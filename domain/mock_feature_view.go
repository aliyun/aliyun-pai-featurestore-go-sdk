package domain

import (
	"context"
	"sync/atomic"

	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/api"
)

// MockFeatureView is a test double for FeatureView that tracks WriteFlush calls.
// It is intended for use in external package tests that need to verify
// FeatureView lifecycle behavior (e.g. goroutine cleanup on metadata refresh).
type MockFeatureView struct {
	FlushCount atomic.Int32
}

func (m *MockFeatureView) GetOnlineFeatures(joinIds []interface{}, features []string, alias map[string]string) ([]map[string]interface{}, error) {
	return nil, nil
}
func (m *MockFeatureView) GetOnlineFeaturesWithContext(ctx context.Context, joinIds []interface{}, features []string, alias map[string]string) ([]map[string]interface{}, error) {
	return nil, nil
}
func (m *MockFeatureView) getOnlineFeaturesWithCount(joinIds []interface{}, features []string, alias map[string]string, count int) ([]map[string]interface{}, error) {
	return nil, nil
}
func (m *MockFeatureView) getOnlineFeaturesWithCountWithContext(ctx context.Context, joinIds []interface{}, features []string, alias map[string]string, count int) ([]map[string]interface{}, error) {
	return nil, nil
}
func (m *MockFeatureView) GetOnlineAggregatedFeatures(joinIds []interface{}, features []string, alias map[string]string) (map[string]interface{}, error) {
	return nil, nil
}
func (m *MockFeatureView) GetOnlineAggregatedFeaturesWithContext(ctx context.Context, joinIds []interface{}, features []string, alias map[string]string) (map[string]interface{}, error) {
	return nil, nil
}
func (m *MockFeatureView) GetBehaviorFeatures(userIds []interface{}, events []interface{}, features []string) ([]map[string]interface{}, error) {
	return nil, nil
}
func (m *MockFeatureView) GetBehaviorFeaturesWithContext(ctx context.Context, userIds []interface{}, events []interface{}, features []string) ([]map[string]interface{}, error) {
	return nil, nil
}
func (m *MockFeatureView) GetOnlineFeaturesWithOptions(joinIds []interface{}, features []string, alias map[string]string, opts FeatureViewOptions) ([]map[string]interface{}, error) {
	return nil, nil
}
func (m *MockFeatureView) GetName() string                          { return "mock" }
func (m *MockFeatureView) GetFeatureEntityName() string             { return "mock_entity" }
func (m *MockFeatureView) GetType() string                          { return "mock" }
func (m *MockFeatureView) Offline2Online(input string) string       { return input }
func (m *MockFeatureView) GetFields() []api.FeatureViewFields       { return nil }
func (m *MockFeatureView) GetIsWriteToFeatureDB() bool              { return false }
func (m *MockFeatureView) GetTTL() int                              { return 0 }
func (m *MockFeatureView) RowCount(string) int                      { return 0 }
func (m *MockFeatureView) RowCountIds(string) ([]string, int, error) { return nil, 0, nil }
func (m *MockFeatureView) ScanAndIterateData(filter string, ch chan<- string) ([]string, error) {
	return nil, nil
}
func (m *MockFeatureView) WriteFeatures(data []map[string]interface{}, opts ...WriteOption) error {
	return nil
}
func (m *MockFeatureView) WriteFeaturesWithInsertMode(data []map[string]interface{}, insertMode string) {}
func (m *MockFeatureView) WriteFlush() {
	m.FlushCount.Add(1)
}
