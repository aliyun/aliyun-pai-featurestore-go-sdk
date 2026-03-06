package domain

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

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
	return f.GetOnlineFeaturesWithContext(context.Background(), joinIds, features, alias)
}

func (f *BaseFeatureView) GetOnlineFeaturesWithContext(ctx context.Context, joinIds []interface{}, features []string, alias map[string]string) ([]map[string]interface{}, error) {
	return f.getOnlineFeaturesWithCountWithContext(ctx, joinIds, features, alias, 1)
}

func (f *BaseFeatureView) getOnlineFeaturesWithCount(joinIds []interface{}, features []string, alias map[string]string, count int) ([]map[string]interface{}, error) {
	return f.getOnlineFeaturesWithCountWithContext(context.Background(), joinIds, features, alias, count)
}

func (f *BaseFeatureView) getOnlineFeaturesWithCountWithContext(ctx context.Context, joinIds []interface{}, features []string, alias map[string]string, count int) ([]map[string]interface{}, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

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

	featureResult, err := f.featureViewDao.GetFeaturesWithContext(ctx, joinIds, selectFields, count)
	if err != nil {
		return nil, err
	}

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

func (f *BaseFeatureView) GetOnlineAggregatedFeaturesWithContext(ctx context.Context, joinIds []interface{}, features []string, alias map[string]string) (map[string]interface{}, error) {
	return nil, errors.New("only sequence feature view supports GetOnlineAggregatedFeatures")
}

func (f *BaseFeatureView) GetBehaviorFeatures(userIds []interface{}, events []interface{}, features []string) ([]map[string]interface{}, error) {
	return nil, errors.New("only sequence feature view supports GetBehaviorFeatures")
}

func (f *BaseFeatureView) GetBehaviorFeaturesWithContext(ctx context.Context, userIds []interface{}, events []interface{}, features []string) ([]map[string]interface{}, error) {
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

func (f *BaseFeatureView) WriteFeatureDB(data []map[string]interface{}) {
	f.featureViewDao.WriteFeatures(data)
}

func (f *BaseFeatureView) WriteFeaturesWithMode(data []map[string]interface{}, insertMode string) {
	if len(data) == 0 {
		return
	}

	filteredData := make([]map[string]interface{}, 0, len(data))

	for _, item := range data {
		filteredItem := f.filterData(item)

		// 只处理非空的数据
		if len(filteredItem) > 0 {
			// 添加插入模式标记
			filteredItem["__insert_mode__"] = insertMode
			filteredData = append(filteredData, filteredItem)
		}
	}

	f.featureViewDao.WriteFeatures(filteredData)
}

func (f *BaseFeatureView) filterData(data map[string]interface{}) map[string]interface{} {
	if len(data) == 0 {
		return make(map[string]interface{})
	}

	filteredMap := make(map[string]interface{})

	for key, value := range data {
		if value == nil {
			continue // 跳过 nil 值
		}

		// 使用反射判断类型
		v := reflect.ValueOf(value)

		switch v.Kind() {
		case reflect.Slice, reflect.Array:
			// 处理 List/Array 类型
			filteredList := f.filterList(value)
			if len(filteredList) > 0 {
				filteredMap[key] = filteredList
			}

		case reflect.Map:
			// 处理 Map 类型
			filteredNestedMap := f.filterMap(value)
			if len(filteredNestedMap) > 0 {
				filteredMap[key] = filteredNestedMap
			}

		default:
			// 其他类型直接保留
			filteredMap[key] = value
		}
	}

	return filteredMap
}

func (f *BaseFeatureView) filterList(list interface{}) []interface{} {
	v := reflect.ValueOf(list)
	if v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
		return []interface{}{}
	}

	result := make([]interface{}, 0, v.Len())

	for i := 0; i < v.Len(); i++ {
		item := v.Index(i).Interface()

		if item == nil {
			continue
		}

		itemValue := reflect.ValueOf(item)

		switch itemValue.Kind() {
		case reflect.Slice, reflect.Array:
			// 嵌套列表
			filtered := f.filterList(item)
			if len(filtered) > 0 {
				result = append(result, filtered)
			}

		case reflect.Map:
			// 嵌套 Map
			filtered := f.filterMap(item)
			if len(filtered) > 0 {
				result = append(result, filtered)
			}

		default:
			// 基本类型
			result = append(result, item)
		}
	}

	return result
}

func (f *BaseFeatureView) filterMap(m interface{}) map[string]interface{} {
	v := reflect.ValueOf(m)
	if v.Kind() != reflect.Map {
		return make(map[string]interface{})
	}

	result := make(map[string]interface{})

	for _, key := range v.MapKeys() {
		keyStr := fmt.Sprintf("%v", key.Interface())
		value := v.MapIndex(key).Interface()

		if value == nil {
			continue
		}

		valueType := reflect.ValueOf(value)

		switch valueType.Kind() {
		case reflect.Slice, reflect.Array:
			// 嵌套列表
			filtered := f.filterList(value)
			if len(filtered) > 0 {
				result[keyStr] = filtered
			}

		case reflect.Map:
			// 嵌套 Map
			filtered := f.filterMap(value)
			if len(filtered) > 0 {
				result[keyStr] = filtered
			}

		default:
			// 基本类型
			result[keyStr] = value
		}
	}

	return result
}

func (f *BaseFeatureView) WriteFeatures(data []map[string]interface{}) error {
	f.featureViewDao.WriteFeatures(data)
	return nil
}

func (f *BaseFeatureView) WriteFeaturesWithInsertMode(data []map[string]interface{}, insertMode string) {
	for _, item := range data {
		item["__insert_mode__"] = insertMode
	}
	f.featureViewDao.WriteFeatures(data)
}

func (f *BaseFeatureView) WriteFlush() {
	f.featureViewDao.WriteFlush()
}

func (f *BaseFeatureView) Close() error {
	return f.featureViewDao.Close()
}
