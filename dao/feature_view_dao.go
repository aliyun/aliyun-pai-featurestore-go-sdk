package dao

import "github.com/aliyun/aliyun-pai-featurestore-go-sdk/swagger/common"

type FeatureViewDao interface {
	GetFeatures(keys []interface{}, selectFields []string) ([]map[string]interface{}, error)
}

func NewFeatureViewDao(config DaoConfig) FeatureViewDao {
	if config.DatasourceType == common.Datasource_Type_Hologres {
		return NewFeatureViewHologresDao(config)
	} else if config.DatasourceType == common.Datasource_Type_Mysql {
		return NewFeatureViewMysqlDao(config)
	} else if config.DatasourceType == common.Datasource_Type_IGraph {
		return NewFeatureViewIGraphDao(config)
	} else if config.DatasourceType == common.Datasource_Type_Redis {
		return NewFeatureViewRedisDao(config)
	} else if config.DatasourceType == common.Datasource_Type_OTS {
		return NewFeatureViewOTSDao(config)
	}

	panic("not found FeatureViewDao implement")
}
