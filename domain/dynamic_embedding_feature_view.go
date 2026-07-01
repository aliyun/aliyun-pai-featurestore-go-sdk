package domain

import (
	"context"
	"errors"

	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/api"
)

var errDynamicEmbeddingNotSupported = errors.New("dynamic embedding feature view does not support this operation")

type DynamicEmbeddingFeatureView struct {
	*api.FeatureView
	Project       *Project
	FeatureEntity *FeatureEntity
}

func NewDynamicEmbeddingFeatureView(view *api.FeatureView, p *Project, entity *FeatureEntity) *DynamicEmbeddingFeatureView {
	return &DynamicEmbeddingFeatureView{
		FeatureView:   view,
		Project:       p,
		FeatureEntity: entity,
	}
}

func (f *DynamicEmbeddingFeatureView) GetName() string {
	return f.Name
}

func (f *DynamicEmbeddingFeatureView) GetFeatureEntityName() string {
	return f.FeatureEntityName
}

func (f *DynamicEmbeddingFeatureView) GetType() string {
	return f.Type
}

func (f *DynamicEmbeddingFeatureView) Offline2Online(input string) string {
	return input
}

func (f *DynamicEmbeddingFeatureView) GetFields() []api.FeatureViewFields {
	fields := make([]api.FeatureViewFields, len(f.Fields))
	for i, field := range f.Fields {
		if field != nil {
			fields[i] = *field
		}
	}
	return fields
}

func (f *DynamicEmbeddingFeatureView) GetIsWriteToFeatureDB() bool {
	return true
}

func (f *DynamicEmbeddingFeatureView) GetTTL() int {
	return f.Ttl
}

func (f *DynamicEmbeddingFeatureView) GetOnlineFeatures(joinIds []interface{}, features []string, alias map[string]string) ([]map[string]interface{}, error) {
	return nil, errDynamicEmbeddingNotSupported
}

func (f *DynamicEmbeddingFeatureView) GetOnlineFeaturesWithContext(ctx context.Context, joinIds []interface{}, features []string, alias map[string]string) ([]map[string]interface{}, error) {
	return nil, errDynamicEmbeddingNotSupported
}

func (f *DynamicEmbeddingFeatureView) getOnlineFeaturesWithCount(joinIds []interface{}, features []string, alias map[string]string, count int) ([]map[string]interface{}, error) {
	return nil, errDynamicEmbeddingNotSupported
}

func (f *DynamicEmbeddingFeatureView) getOnlineFeaturesWithCountWithContext(ctx context.Context, joinIds []interface{}, features []string, alias map[string]string, count int) ([]map[string]interface{}, error) {
	return nil, errDynamicEmbeddingNotSupported
}

func (f *DynamicEmbeddingFeatureView) GetOnlineFeaturesWithOptions(joinIds []interface{}, features []string, alias map[string]string, opts FeatureViewOptions) ([]map[string]interface{}, error) {
	return nil, errDynamicEmbeddingNotSupported
}

func (f *DynamicEmbeddingFeatureView) GetOnlineAggregatedFeatures(joinIds []interface{}, features []string, alias map[string]string) (map[string]interface{}, error) {
	return nil, errDynamicEmbeddingNotSupported
}

func (f *DynamicEmbeddingFeatureView) GetOnlineAggregatedFeaturesWithContext(ctx context.Context, joinIds []interface{}, features []string, alias map[string]string) (map[string]interface{}, error) {
	return nil, errDynamicEmbeddingNotSupported
}

func (f *DynamicEmbeddingFeatureView) GetBehaviorFeatures(userIds []interface{}, events []interface{}, features []string) ([]map[string]interface{}, error) {
	return nil, errDynamicEmbeddingNotSupported
}

func (f *DynamicEmbeddingFeatureView) GetBehaviorFeaturesWithContext(ctx context.Context, userIds []interface{}, events []interface{}, features []string) ([]map[string]interface{}, error) {
	return nil, errDynamicEmbeddingNotSupported
}

func (f *DynamicEmbeddingFeatureView) RowCount(string) int {
	return 0
}

func (f *DynamicEmbeddingFeatureView) RowCountIds(string) ([]string, int, error) {
	return nil, 0, errDynamicEmbeddingNotSupported
}

func (f *DynamicEmbeddingFeatureView) ScanAndIterateData(filter string, ch chan<- string) ([]string, error) {
	return nil, errDynamicEmbeddingNotSupported
}

func (f *DynamicEmbeddingFeatureView) WriteFeatures(data []map[string]interface{}, opts ...WriteOption) error {
	return errDynamicEmbeddingNotSupported
}

func (f *DynamicEmbeddingFeatureView) WriteFeaturesWithInsertMode(data []map[string]interface{}, insertMode string) {
}

func (f *DynamicEmbeddingFeatureView) WriteFlush() {
}

// compile-time interface check
var _ FeatureView = (*DynamicEmbeddingFeatureView)(nil)
