package dao

import (
	"errors"
	"strconv"

	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/constants"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/datasource/ots"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
)

type FeatureViewOTSDao struct {
	otsClient       *tablestore.TableStoreClient
	table           string
	primaryKeyField string
	eventTimeField  string
	ttl             int
	fieldTypeMap    map[string]constants.FSType
}

func NewFeatureViewOTSDao(config DaoConfig) *FeatureViewOTSDao {
	dao := FeatureViewOTSDao{
		table:           config.OtsTableName,
		primaryKeyField: config.PrimaryKeyField,
		eventTimeField:  config.EventTimeField,
		ttl:             config.TTL,
		fieldTypeMap:    config.FieldTypeMap,
	}
	client, err := ots.GetOTSClient(config.OtsName)
	if err != nil {
		return nil
	}

	dao.otsClient = client.GetClient()
	return &dao

}

func (d *FeatureViewOTSDao) GetFeatures(keys []interface{}, selectFields []string) ([]map[string]interface{}, error) {
	result := make([]map[string]interface{}, 0, len(keys))
	batchGetReq := &tablestore.BatchGetRowRequest{}
	mqCriteria := &tablestore.MultiRowQueryCriteria{}

	for _, key := range keys {
		pkToGet := new(tablestore.PrimaryKey)
		if d.fieldTypeMap[d.primaryKeyField] == constants.FS_INT64 || d.fieldTypeMap[d.primaryKeyField] == constants.FS_INT32 {
			if v, ok := key.(int64); ok {
				pkToGet.AddPrimaryKeyColumn(d.primaryKeyField, v)
			} else {
				s, _ := key.(string)
				i, _ := strconv.ParseInt(s, 10, 64)
				pkToGet.AddPrimaryKeyColumn(d.primaryKeyField, i)
			}
		} else if d.fieldTypeMap[d.primaryKeyField] == constants.FS_STRING {
			pkToGet.AddPrimaryKeyColumn(d.primaryKeyField, key)
		} else {
			return result, errors.New("primary key type is not supported by OTS")
		}
		mqCriteria.AddRow(pkToGet)
		mqCriteria.MaxVersion = 1
		mqCriteria.ColumnsToGet = selectFields
	}

	mqCriteria.TableName = d.table
	batchGetReq.MultiRowQueryCriteria = append(batchGetReq.MultiRowQueryCriteria, mqCriteria)
	batchGetResponse, err := d.otsClient.BatchGetRow(batchGetReq)

	if err != nil {
		return result, err
	}

	for _, rowResults := range batchGetResponse.TableToRowsResult {
		for _, rowResult := range rowResults {
			if rowResult.Error.Message != "" {
				return result, errors.New(rowResult.Error.Message)
			}
			newMap := make(map[string]interface{})
			for _, pkValue := range rowResult.PrimaryKey.PrimaryKeys {
				newMap[pkValue.ColumnName] = pkValue.Value
			}
			for _, rowValue := range rowResult.Columns {
				newMap[rowValue.ColumnName] = rowValue.Value
			}
			result = append(result, newMap)
		}
	}

	return result, nil
}
