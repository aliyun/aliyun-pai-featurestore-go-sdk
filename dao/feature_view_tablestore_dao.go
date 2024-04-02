package dao

import (
	"errors"
	"fmt"
	"log"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/api"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/constants"
	fstablestore "github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/datasource/tablestore"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/utils"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
)

type FeatureViewTableStoreDao struct {
	tablestoreClient *tablestore.TableStoreClient
	table            string
	primaryKeyField  string
	eventTimeField   string
	ttl              int
	fieldTypeMap     map[string]constants.FSType

	offlineTable string
	onlineTable  string
}

func NewFeatureViewTableStoreDao(config DaoConfig) *FeatureViewTableStoreDao {
	dao := FeatureViewTableStoreDao{
		table:           config.TableStoreTableName,
		primaryKeyField: config.PrimaryKeyField,
		eventTimeField:  config.EventTimeField,
		ttl:             config.TTL,
		fieldTypeMap:    config.FieldTypeMap,
		offlineTable:    config.TableStoreOfflineTableName,
		onlineTable:     config.TableStoreOnlineTableName,
	}
	client, err := fstablestore.GetTableStoreClient(config.TableStoreName)
	if err != nil {
		return nil
	}

	dao.tablestoreClient = client.GetClient()
	return &dao

}

func (d *FeatureViewTableStoreDao) GetFeatures(keys []interface{}, selectFields []string) ([]map[string]interface{}, error) {
	result := make([]map[string]interface{}, 0, len(keys))
	var wg sync.WaitGroup
	var mu sync.Mutex

	for i := 0; i < len(keys); i += 100 {
		end := i + 100
		if end > len(keys) {
			end = len(keys)
		}
		ks := keys[i:end]
		wg.Add(1)
		go func(ks []interface{}) {
			defer wg.Done()
			batchGetReq := &tablestore.BatchGetRowRequest{}
			mqCriteria := &tablestore.MultiRowQueryCriteria{}

			for _, key := range ks {
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
					log.Println(errors.New("primary key type is not supported by TableStore"))
					return
				}
				mqCriteria.AddRow(pkToGet)
				mqCriteria.MaxVersion = 1
				mqCriteria.ColumnsToGet = selectFields
			}

			mqCriteria.TableName = d.table
			batchGetReq.MultiRowQueryCriteria = append(batchGetReq.MultiRowQueryCriteria, mqCriteria)
			batchGetResponse, err := d.tablestoreClient.BatchGetRow(batchGetReq)

			if err != nil {
				log.Println(err)
				return
			}

			for _, rowResults := range batchGetResponse.TableToRowsResult {
				for _, rowResult := range rowResults {
					if rowResult.Error.Message != "" {
						log.Println(errors.New(rowResult.Error.Message))
						return
					}
					newMap := make(map[string]interface{})
					for _, pkValue := range rowResult.PrimaryKey.PrimaryKeys {
						newMap[pkValue.ColumnName] = pkValue.Value
					}
					for _, rowValue := range rowResult.Columns {
						newMap[rowValue.ColumnName] = rowValue.Value
					}
					mu.Lock()
					result = append(result, newMap)
					mu.Unlock()
				}
			}
		}(ks)
	}
	wg.Wait()

	return result, nil
}

func (d *FeatureViewTableStoreDao) GetUserSequenceFeature(keys []interface{}, userIdField string, sequenceConfig api.FeatureViewSeqConfig, onlineConfig []*api.SeqConfig) ([]map[string]interface{}, error) {
	currTime := time.Now().Unix()
	sequencePlayTimeMap := makePlayTimeMap(sequenceConfig)

	pkField := fmt.Sprintf("%s_%s", userIdField, sequenceConfig.EventField)
	var skField string
	if sequenceConfig.DeduplicationMethodNum == 1 {
		skField = sequenceConfig.ItemIdField
	} else if sequenceConfig.DeduplicationMethodNum == 2 {
		skField = fmt.Sprintf("%s_%s", sequenceConfig.ItemIdField, sequenceConfig.TimestampField)
	}

	fetchDataFunc := func(seqEvent string, seqLen int, key interface{}, tableName string) []*sequenceInfo {
		sequences := []*sequenceInfo{}

		getRangeRequest := &tablestore.GetRangeRequest{}
		rangeRowQueryCriteria := &tablestore.RangeRowQueryCriteria{}
		rangeRowQueryCriteria.TableName = tableName

		startPK := new(tablestore.PrimaryKey)
		startPK.AddPrimaryKeyColumn(pkField, fmt.Sprintf("%v_%s", key, seqEvent))
		startPK.AddPrimaryKeyColumnWithMinValue(skField)
		endPK := new(tablestore.PrimaryKey)
		endPK.AddPrimaryKeyColumn(pkField, fmt.Sprintf("%v_%s", key, seqEvent))
		endPK.AddPrimaryKeyColumnWithMaxValue(skField)

		rangeRowQueryCriteria.StartPrimaryKey = startPK
		rangeRowQueryCriteria.EndPrimaryKey = endPK
		rangeRowQueryCriteria.Direction = tablestore.FORWARD
		rangeRowQueryCriteria.ColumnsToGet = []string{sequenceConfig.ItemIdField, sequenceConfig.EventField, sequenceConfig.PlayTimeField, sequenceConfig.TimestampField}
		timeRange := new(tablestore.TimeRange)
		timeRange.End = currTime * 1000
		timeRange.Start = (currTime - 86400*5) * 1000
		rangeRowQueryCriteria.TimeRange = timeRange

		getRangeRequest.RangeRowQueryCriteria = rangeRowQueryCriteria
		getRangeResp, err := d.tablestoreClient.GetRange(getRangeRequest)

		for {
			if err != nil {
				fmt.Println("get range failed with error:", err)
			}
			for _, row := range getRangeResp.Rows {
				if row.PrimaryKey.PrimaryKeys == nil {
					continue
				}
				seq := new(sequenceInfo)
				if sequenceConfig.DeduplicationMethodNum == 1 {
					seq.itemId = utils.ToString(row.PrimaryKey.PrimaryKeys[1].Value, "")
				}
				for _, column := range row.Columns {
					switch column.ColumnName {
					case sequenceConfig.EventField:
						seq.event = utils.ToString(column.Value, "")
					case sequenceConfig.ItemIdField:
						seq.itemId = utils.ToString(column.Value, "")
					case sequenceConfig.PlayTimeField:
						seq.playTime = utils.ToFloat(column.Value, 0)
					case sequenceConfig.TimestampField:
						seq.timestamp = utils.ToInt64(column.Value, 0)
					}
				}

				if seq.event == "" || seq.itemId == "" {
					continue
				}
				if t, exist := sequencePlayTimeMap[seqEvent]; exist {
					if seq.playTime <= t {
						continue
					}
				}

				sequences = append(sequences, seq)
			}
			if getRangeResp.NextStartPrimaryKey == nil {
				break
			} else {
				getRangeRequest.RangeRowQueryCriteria.StartPrimaryKey = getRangeResp.NextStartPrimaryKey
				getRangeResp, err = d.tablestoreClient.GetRange(getRangeRequest)
			}
		}

		// add seqLen limit
		sort.Slice(sequences, func(i, j int) bool {
			return sequences[i].timestamp > sequences[j].timestamp
		})
		limit := seqLen
		if seqLen > len(sequences) {
			limit = len(sequences)
		}

		resultSequences := sequences[:limit]

		return resultSequences
	}

	results := make([]map[string]interface{}, 0, len(keys))
	var outmu sync.Mutex

	var wg sync.WaitGroup
	for _, key := range keys {
		wg.Add(1)
		go func(key interface{}) {
			defer wg.Done()
			properties := make(map[string]interface{})
			var mu sync.Mutex

			var eventWg sync.WaitGroup
			for _, seqConfig := range onlineConfig {
				eventWg.Add(1)
				go func(seqConfig *api.SeqConfig) {
					defer eventWg.Done()
					var onlineSequences []*sequenceInfo
					var offlineSequences []*sequenceInfo

					var innerWg sync.WaitGroup
					//get data from online table
					innerWg.Add(1)
					go func(seqEvent string, seqLen int, key interface{}) {
						defer innerWg.Done()
						if onlineresult := fetchDataFunc(seqEvent, seqLen, key, d.onlineTable); onlineresult != nil {
							onlineSequences = onlineresult
						}
					}(seqConfig.SeqEvent, seqConfig.SeqLen, key)
					//get data from offline table
					innerWg.Add(1)
					go func(seqEvent string, seqLen int, key interface{}) {
						defer innerWg.Done()
						if offlineresult := fetchDataFunc(seqEvent, seqLen, key, d.offlineTable); offlineresult != nil {
							offlineSequences = offlineresult
						}
					}(seqConfig.SeqEvent, seqConfig.SeqLen, key)
					innerWg.Wait()

					subproperties := makeSequenceFeatures(offlineSequences, onlineSequences, seqConfig, sequenceConfig, currTime)
					mu.Lock()
					defer mu.Unlock()
					for k, value := range subproperties {
						properties[k] = value
					}
				}(seqConfig)
			}
			eventWg.Wait()

			properties[userIdField] = key
			outmu.Lock()
			results = append(results, properties)
			outmu.Unlock()
		}(key)
	}

	wg.Wait()

	return results, nil
}
