package domain

import (
	"encoding/json"
	"fmt"

	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/constants"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/dao"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/swagger"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/swagger/common"
)

type FeatureView struct {
	*swagger.FeatureView
	Project         *Project
	FeatureEntity   *FeatureEntity
	featureFields   []string
	primaryKeyField swagger.FeatureViewFields
	eventTimeField  swagger.FeatureViewFields
	featureViewDao  dao.FeatureViewDao
}

func NewFeatureView(view *swagger.FeatureView, p *Project, entity *FeatureEntity) *FeatureView {
	featureView := &FeatureView{
		FeatureView:   view,
		Project:       p,
		FeatureEntity: entity,
	}
	for _, field := range view.Fields {
		if field.IsEventTime {
			featureView.eventTimeField = field
		} else if field.IsPartition {
			continue
		} else if field.IsPrimaryKey {
			featureView.primaryKeyField = field
		} else {
			featureView.featureFields = append(featureView.featureFields, field.Name)
		}
	}

	daoConfig := dao.DaoConfig{
		DatasourceType:    p.OnlineDatasourceType,
		PrimaryKeyField:   featureView.primaryKeyField.Name,
		EventTimeField:    featureView.eventTimeField.Name,
		TTL:               int(featureView.Ttl),
		SaveOriginalField: false,
	}
	switch p.OnlineDatasourceType {
	case common.Datasource_Type_Hologres:
		daoConfig.HologresTableName = p.OnlineStore.GetTableName(featureView)
		daoConfig.HologresName = p.OnlineStore.GetDatasourceName()
	case common.Datasource_Type_Mysql:
		daoConfig.MysqlTableName = p.OnlineStore.GetTableName(featureView)
		daoConfig.MysqlName = p.OnlineStore.GetDatasourceName()
	case common.Datasource_Type_IGraph:
		if view.Config != "" {
			configM := make(map[string]interface{})
			if err := json.Unmarshal([]byte(view.Config), &configM); err == nil {
				if save_original_field, exist := configM["save_original_field"]; exist {
					if val, ok := save_original_field.(bool); ok {
						daoConfig.SaveOriginalField = val
					}
				}
			}
		}
		daoConfig.IGraphName = p.OnlineStore.GetDatasourceName()
		daoConfig.GroupName = p.ProjectName
		daoConfig.LabelName = featureView.Name
		fieldMap := make(map[string]string, len(view.Fields))
		fieldTypeMap := make(map[string]constants.FSType, len(view.Fields))
		for _, field := range view.Fields {
			if field.IsPrimaryKey {
				fieldMap[field.Name] = field.Name
				fieldTypeMap[field.Name] = constants.FSType(field.Type)
			} else if field.IsPartition {
				continue
			} else {
				var name string
				if daoConfig.SaveOriginalField {
					name = field.Name
				} else {
					name = fmt.Sprintf("f%d", field.Position)
				}

				fieldMap[name] = field.Name
				fieldTypeMap[name] = constants.FSType(field.Type)
			}
		}
		daoConfig.FieldMap = fieldMap
		daoConfig.FieldTypeMap = fieldTypeMap
	case common.Datasource_Type_Redis:
		daoConfig.RedisPrefix = p.OnlineStore.GetTableName(featureView)
		daoConfig.RedisName = p.OnlineStore.GetDatasourceName()
		fields := make([]string, 0, len(view.Fields))
		fieldTypeMap := make(map[string]constants.FSType, len(view.Fields))
		for _, field := range view.Fields {
			if field.IsPrimaryKey {
				fieldTypeMap[field.Name] = constants.FSType(field.Type)
			} else if field.IsPartition {
				continue
			} else {
				fields = append(fields, field.Name)
				fieldTypeMap[field.Name] = constants.FSType(field.Type)
			}
		}
		daoConfig.Fields = fields
		daoConfig.FieldTypeMap = fieldTypeMap
	default:
	}
	featureViewDao := dao.NewFeatureViewDao(daoConfig)
	featureView.featureViewDao = featureViewDao

	return featureView
}

func (f *FeatureView) GetOnlineFeatures(joinIds []interface{}, features []string, alias map[string]string) ([]map[string]interface{}, error) {
	var selectFields []string
	selectFields = append(selectFields, f.primaryKeyField.Name)
	for _, featureName := range features {
		if featureName == "*" {
			selectFields = append(selectFields, f.featureFields...)
		} else {
			found := false
			for _, field := range f.featureFields {
				if field == featureName {
					found = true
					break
				}
			}
			if !found {
				return nil, fmt.Errorf("feature name :%s not found in the featureview fields", featureName)
			}

			selectFields = append(selectFields, featureName)
		}
	}

	for featureName := range alias {
		found := false

		for _, field := range f.featureFields {
			if field == featureName {
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("feature name :%s not found in the featureview fields", featureName)
		}
	}

	featureResult, err := f.featureViewDao.GetFeatures(joinIds, selectFields)

	if f.primaryKeyField.Name != f.FeatureEntity.FeatureEntityJoinid {
		for _, featureMap := range featureResult {
			featureMap[f.FeatureEntity.FeatureEntityJoinid] = featureMap[f.primaryKeyField.Name]
			delete(featureMap, f.primaryKeyField.Name)
		}
	}

	for featureName, aliasName := range alias {
		for _, featureMap := range featureResult {
			if _, ok := featureMap[featureName]; ok {
				featureMap[aliasName] = featureMap[featureName]
				delete(featureMap, featureName)
			}
		}
	}

	return featureResult, err

}
