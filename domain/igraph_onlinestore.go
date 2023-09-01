package domain

import "github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/api"

type IGraphOnlineStore struct {
	*api.Datasource
}

func (s *IGraphOnlineStore) GetTableName(featureView *FeatureView) string {
	return featureView.Name
}

func (s *IGraphOnlineStore) GetDatasourceName() string {
	return s.Name
}
