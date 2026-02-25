package domain

import (
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/api"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/constants"
)

type FeatureView interface {
	GetOnlineFeatures(joinIds []interface{}, features []string, alias map[string]string) ([]map[string]interface{}, error)
	getOnlineFeaturesWithCount(joinIds []interface{}, features []string, alias map[string]string, count int) ([]map[string]interface{}, error)
	GetOnlineAggregatedFeatures(joinIds []interface{}, features []string, alias map[string]string) (map[string]interface{}, error)
	GetBehaviorFeatures(userIds []interface{}, events []interface{}, features []string) ([]map[string]interface{}, error)
	GetName() string
	GetFeatureEntityName() string
	GetType() string
	Offline2Online(input string) string
	GetFields() []api.FeatureViewFields
	GetIsWriteToFeatureDB() bool
	GetTTL() int

	// RowCount gets the count filter by the given expression
	RowCount(string) int

	//RowCountIds gets the primary key list and  count filter by the given expression
	RowCountIds(expr string) ([]string, int, error)

	// ScanAndIterateData gets the primary key list  by the given expression
	// If stream feature view can iterate the data deliver to the channel
	ScanAndIterateData(filter string, ch chan<- string) ([]string, error)
}

func NewFeatureView(view *api.FeatureView, p *Project, entity *FeatureEntity) FeatureView {
	if view.Type == constants.Feature_View_Type_Sequence {
		return NewSequenceFeatureView(view, p, entity)
	} else {
		return NewBaseFeatureView(view, p, entity)
	}
}
