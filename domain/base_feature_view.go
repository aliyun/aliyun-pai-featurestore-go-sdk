package domain

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/api"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/constants"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/dao"
)

type BaseFeatureView struct {
	*api.FeatureView
	Project         *Project
	FeatureEntity   *FeatureEntity
	featureFields   []string
	primaryKeyField api.FeatureViewFields
	eventTimeField  api.FeatureViewFields
	featureViewDao  dao.FeatureViewDao
}

func NewBaseFeatureView(view *api.FeatureView, p *Project, entity *FeatureEntity) *BaseFeatureView {
	featureView := &BaseFeatureView{
		FeatureView:   view,
		Project:       p,
		FeatureEntity: entity,
	}
	for _, field := range view.Fields {
		if field.IsEventTime {
			featureView.eventTimeField = *field
			featureView.featureFields = append(featureView.featureFields, field.Name)
		} else if field.IsPartition {
			continue
		} else if field.IsPrimaryKey {
			featureView.primaryKeyField = *field
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

	if view.WriteToFeatureDB || p.OnlineDatasourceType == constants.Datasource_Type_FeatureDB {
		daoConfig.DatasourceType = constants.Datasource_Type_FeatureDB
		daoConfig.FeatureDBDatabaseName = p.InstanceId
		daoConfig.FeatureDBSchemaName = p.ProjectName
		daoConfig.FeatureDBTableName = featureView.Name
		daoConfig.FeatureDBSignature = p.Signature

		fieldTypeMap := make(map[string]constants.FSType, len(view.Fields))
		for _, field := range view.Fields {
			if field.IsPartition {
				continue
			} else {
				fieldTypeMap[field.Name] = field.Type
			}
		}
		daoConfig.FieldTypeMap = fieldTypeMap
		daoConfig.Fields = featureView.featureFields
	} else {
		switch p.OnlineDatasourceType {
		case constants.Datasource_Type_Hologres:
			daoConfig.HologresTableName = p.OnlineStore.GetTableName(featureView)
			daoConfig.HologresName = p.OnlineStore.GetDatasourceName()
		case constants.Datasource_Type_IGraph:
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
			daoConfig.LabelName = p.OnlineStore.GetTableName(featureView)
			fieldMap := make(map[string]string, len(view.Fields))
			fieldTypeMap := make(map[string]constants.FSType, len(view.Fields))
			for _, field := range view.Fields {
				if field.IsPrimaryKey {
					fieldMap[field.Name] = field.Name
					fieldTypeMap[field.Name] = field.Type
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
					fieldTypeMap[name] = field.Type
				}
			}
			daoConfig.FieldMap = fieldMap
			daoConfig.FieldTypeMap = fieldTypeMap
		case constants.Datasource_Type_TableStore:
			daoConfig.TableStoreTableName = p.OnlineStore.GetTableName(featureView)
			daoConfig.TableStoreName = p.OnlineStore.GetDatasourceName()
			fieldTypeMap := make(map[string]constants.FSType, len(view.Fields))
			for _, field := range view.Fields {
				if field.IsPrimaryKey {
					fieldTypeMap[field.Name] = field.Type
				} else if field.IsPartition {
					continue
				} else {
					fieldTypeMap[field.Name] = field.Type
				}
			}
			daoConfig.FieldTypeMap = fieldTypeMap

		default:

		}
	}

	featureViewDao := dao.NewFeatureViewDao(daoConfig)
	featureView.featureViewDao = featureViewDao

	return featureView
}

func (f *BaseFeatureView) GetOnlineFeatures(joinIds []interface{}, features []string, alias map[string]string) ([]map[string]interface{}, error) {
	var selectFields []string
	selectFields = append(selectFields, f.primaryKeyField.Name)
	seenFields := make(map[string]bool)
	seenFields[f.primaryKeyField.Name] = true
	for _, featureName := range features {
		if featureName == "*" {
			selectFields = append(selectFields, f.featureFields...)
		} else {
			if seenFields[featureName] {
				continue
			}
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
			seenFields[featureName] = true
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

	featureResult, err := f.featureViewDao.GetFeatures(joinIds, selectFields, 1)

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

func (f *BaseFeatureView) getOnlineFeaturesWithCount(joinIds []interface{}, features []string, alias map[string]string, count int) ([]map[string]interface{}, error) {
	var selectFields []string
	selectFields = append(selectFields, f.primaryKeyField.Name)
	seenFields := make(map[string]bool)
	seenFields[f.primaryKeyField.Name] = true
	for _, featureName := range features {
		if featureName == "*" {
			selectFields = append(selectFields, f.featureFields...)
		} else {
			if seenFields[featureName] {
				continue
			}
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
			seenFields[featureName] = true
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

	featureResult, err := f.featureViewDao.GetFeatures(joinIds, selectFields, count)

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

func (f *BaseFeatureView) GetOnlineAggregatedFeatures(joinIds []interface{}, features []string, alias map[string]string) (map[string]interface{}, error) {
	return nil, errors.New("only sequence feature view supports GetOnlineAggregatedFeatures")
}

func (f *BaseFeatureView) GetBehaviorFeatures(userIds []interface{}, events []interface{}, features []string) ([]map[string]interface{}, error) {
	return nil, errors.New("only sequence feature view supports GetBehaviorFeatures")
}

func (f *BaseFeatureView) GetName() string {
	return f.Name
}

func (f *BaseFeatureView) GetFeatureEntityName() string {
	return f.FeatureEntityName
}

func (f *BaseFeatureView) GetType() string {
	return f.Type
}

func (f *BaseFeatureView) Offline2Online(input string) string {
	return input
}

func (f *BaseFeatureView) GetFields() []api.FeatureViewFields {
	fields := make([]api.FeatureViewFields, len(f.Fields))
	for i, field := range f.Fields {
		if field != nil {
			fields[i] = *field
		}
	}
	return fields
}

func (f *BaseFeatureView) GetIsWriteToFeatureDB() bool {
	return f.WriteToFeatureDB || f.Project.OnlineDatasourceType == constants.Datasource_Type_FeatureDB
}

func (f *BaseFeatureView) GetTTL() int {
	return f.Ttl
}

func (f *BaseFeatureView) RowCount(expr string) int {
	return f.featureViewDao.RowCount(expr)
}

// RowCountIds implements FeatureView.
func (f *BaseFeatureView) RowCountIds(expr string) ([]string, int, error) {
	return f.featureViewDao.RowCountIds(expr)
}

// ScanAndIterateData implements FeatureView.
func (f *BaseFeatureView) ScanAndIterateData(filter string, ch chan<- string) ([]string, error) {
	return f.featureViewDao.ScanAndIterateData(filter, ch)
}
