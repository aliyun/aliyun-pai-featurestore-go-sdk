package domain

import (
	"fmt"

	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/swagger"
)

type OTSOnlineStore struct {
	*swagger.Datasource
}

func (s *OTSOnlineStore) GetTableName(featureView *FeatureView) string {
	project := featureView.Project
	return fmt.Sprintf("%s_%s_online", project.ProjectName, featureView.Name)
}

func (s *OTSOnlineStore) GetDatasourceName() string {
	return s.Name
}
