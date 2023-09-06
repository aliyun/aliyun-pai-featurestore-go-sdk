package domain

import (
	"fmt"

	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/api"
)

type IGraphOnlineStore struct {
	*api.Datasource
}

func (s *IGraphOnlineStore) GetTableName(featureView *FeatureView) string {
	return fmt.Sprintf("%s_fv%d", featureView.FeatureEntityName, featureView.FeatureViewId)
}

func (s *IGraphOnlineStore) GetDatasourceName() string {
	return s.Name
}
