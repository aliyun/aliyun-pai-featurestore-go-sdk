package domain

import "github.com/aliyun/aliyun-pai-featurestore-go-sdk/swagger"

type IGraphOnlineStore struct {
	*swagger.Datasource
}

func (s *IGraphOnlineStore) GetTableName(featureView *FeatureView) string {
	return featureView.Name
}

func (s *IGraphOnlineStore) GetDatasourceName() string {
	return s.Name
}
