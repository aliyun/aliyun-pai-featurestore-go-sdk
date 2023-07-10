package domain

import (
	"fmt"

	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/swagger"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/utils"
)

type RedisOnlineStore struct {
	*swagger.Datasource
}

func (s *RedisOnlineStore) GetTableName(featureView *FeatureView) string {
	project := featureView.Project
	name := fmt.Sprintf("%s_%s_online", project.ProjectName, featureView.Name)
	md5 := utils.Md5(name)
	return md5[:4] + "_"
}

func (s *RedisOnlineStore) GetDatasourceName() string {
	return s.Name
}
