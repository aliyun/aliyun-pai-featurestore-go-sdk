package domain

import (
	"fmt"

	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/swagger"
)

type MysqlOnlineStore struct {
	*swagger.Datasource
}

func (s *MysqlOnlineStore) GetTableName(featureView *FeatureView) string {
	project := featureView.Project
	return fmt.Sprintf("%s_%s_online", project.ProjectName, featureView.Name)
}

func (s *MysqlOnlineStore) GetDatasourceName() string {
	return s.Name
}
