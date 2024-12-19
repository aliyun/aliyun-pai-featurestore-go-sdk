package dao

import (
	"errors"
	"fmt"
	"log"
	"net/url"
	"strings"
	"sync"
	"time"

	aligraph "github.com/aliyun/aliyun-igraph-go-sdk"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/api"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/constants"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/datasource/igraph"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/utils"
)

type FeatureViewIGraphDao struct {
	UnimplementedFeatureViewDao
	igraphClient    *aligraph.Client
	group           string
	label           string
	primaryKeyField string
	eventTimeField  string
	ttl             int
	fieldMap        map[string]string
	fieldTypeMap    map[string]constants.FSType
	reverseFieldMap map[string]string

	edgeName string
}

func NewFeatureViewIGraphDao(config DaoConfig) *FeatureViewIGraphDao {
	dao := FeatureViewIGraphDao{
		group:           config.GroupName,
		label:           config.LabelName,
		primaryKeyField: config.PrimaryKeyField,
		eventTimeField:  config.EventTimeField,
		ttl:             config.TTL,
		fieldMap:        config.FieldMap, // igraph name => feature view schema name mapping
		fieldTypeMap:    config.FieldTypeMap,
		reverseFieldMap: make(map[string]string, len(config.FieldMap)), // revserse fieldMap kv, feature view schema name => igraph name mapping
		edgeName:        config.IgraphEdgeName,
	}
	client, err := igraph.GetGraphClient(config.IGraphName)
	if err != nil {
		return nil
	}

	dao.igraphClient = client.GraphClient
	for k, v := range dao.fieldMap {
		dao.reverseFieldMap[v] = k
	}
	return &dao
}
func (d *FeatureViewIGraphDao) GetFeatures(keys []interface{}, selectFields []string) ([]map[string]interface{}, error) {
	var pkeys []string
	for _, key := range keys {
		if pkey := utils.ToString(key, ""); pkey != "" {
			pkeys = append(pkeys, url.QueryEscape(pkey))
		}
	}
	selector := make([]string, 0, len(selectFields))
	for _, field := range selectFields {
		selector = append(selector, fmt.Sprintf("\"%s\"", d.reverseFieldMap[field]))
	}

	var queryString string
	if len(d.fieldMap) == len(selectFields) {
		queryString = fmt.Sprintf("g(\"%s\").V(\"%s\").hasLabel(\"%s\")", d.group, strings.Join(pkeys, ";"), d.label)
	} else {
		queryString = fmt.Sprintf("g(\"%s\").V(\"%s\").hasLabel(\"%s\").fields(%s)", d.group, strings.Join(pkeys, ";"), d.label, strings.Join(selector, ","))
	}

	request := aligraph.ReadRequest{
		QueryString: queryString,
	}
	resp, err := d.igraphClient.Read(&request)
	if err != nil {
		return nil, err
	}

	result := make([]map[string]interface{}, 0, len(keys))
	for _, resultData := range resp.Result {
		for _, data := range resultData.Data {
			properties := make(map[string]interface{}, len(data))

			for field, value := range data {
				if field == "label" {
					continue
				}

				switch d.fieldTypeMap[field] {
				case constants.FS_DOUBLE, constants.FS_FLOAT:
					properties[d.fieldMap[field]] = utils.ToFloat(value, -1024)
				case constants.FS_INT32, constants.FS_INT64:
					properties[d.fieldMap[field]] = utils.ToInt(value, -1024)
				default:
					properties[d.fieldMap[field]] = value
				}
			}

			result = append(result, properties)
		}
	}

	return result, nil
}

func (d *FeatureViewIGraphDao) GetUserSequenceFeature(keys []interface{}, userIdField string, sequenceConfig api.FeatureViewSeqConfig, onlineConfig []*api.SeqConfig) ([]map[string]interface{}, error) {
	var selectFields []string
	if sequenceConfig.PlayTimeField == "" {
		selectFields = []string{sequenceConfig.ItemIdField, sequenceConfig.EventField, sequenceConfig.TimestampField}
	} else {
		selectFields = []string{sequenceConfig.ItemIdField, sequenceConfig.EventField, sequenceConfig.PlayTimeField, sequenceConfig.TimestampField}
	}

	currTime := time.Now().Unix()
	sequencePlayTimeMap := makePlayTimeMap(sequenceConfig.PlayTimeFilter)

	fetchDataFunc := func(seqEvent string, seqLen int, key interface{}) []*sequenceInfo {
		sequences := []*sequenceInfo{}
		events := strings.Split(seqEvent, "|")
		var pk string
		if len(events) > 1 {
			pks := make([]string, len(events))
			for i, event := range events {
				pks[i] = url.QueryEscape(fmt.Sprintf("%v_%s", key, event))
			}
			pk = strings.Join(pks, ";")
		} else {
			pk = url.QueryEscape(fmt.Sprintf("%v_%s", key, seqEvent))
		}
		queryString := fmt.Sprintf("g(\"%s\").E(\"%s\").hasLabel(\"%s\").fields(\"%s\").order().by(\"%s\",Order.decr).limit(%d)",
			d.group, pk, d.edgeName, strings.Join(selectFields, ";"), sequenceConfig.TimestampField, seqLen)
		request := aligraph.ReadRequest{
			QueryString: queryString,
		}
		resp, err := d.igraphClient.Read(&request)
		if err != nil {
			log.Println(err)
			return nil
		}

		for _, resultData := range resp.Result {
			for _, data := range resultData.Data {
				seq := new(sequenceInfo)
				for field, value := range data {
					if field == "label" {
						continue
					}
					switch field {
					case sequenceConfig.EventField:
						seq.event = utils.ToString(value, "")
					case sequenceConfig.ItemIdField:
						seq.itemId = utils.ToString(value, "")
					case sequenceConfig.PlayTimeField:
						seq.playTime = utils.ToFloat(value, 0)
					case sequenceConfig.TimestampField:
						seq.timestamp = utils.ToInt64(value, 0)
					default:
					}
				}

				if seq.event == "" || seq.itemId == "" {
					continue
				}
				if t, exist := sequencePlayTimeMap[seq.event]; exist {
					if seq.playTime <= t {
						continue
					}
				}

				sequences = append(sequences, seq)
			}
		}

		return sequences
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

					//get data from edge

					if onlineresult := fetchDataFunc(seqConfig.SeqEvent, seqConfig.SeqLen, key); onlineresult != nil {
						onlineSequences = onlineresult
					}

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

func (d *FeatureViewIGraphDao) GetUserBehaviorFeature(userIds []interface{}, events []interface{}, selectFields []string, sequenceConfig api.FeatureViewSeqConfig) ([]map[string]interface{}, error) {
	if len(events) == 0 {
		return []map[string]interface{}{}, errors.New("igraph not support GetBehaviorFeatures with empty events")
	}
	sequencePlayTimeMap := makePlayTimeMap(sequenceConfig.PlayTimeFilter)

	fetchDataFunc := func(userId interface{}) []map[string]interface{} {
		var queryString string
		var pkeys []string
		for _, event := range events {
			pkeys = append(pkeys, url.QueryEscape(fmt.Sprintf("%v_%v", userId, event)))
		}
		queryString = fmt.Sprintf("g(\"%s\").E(\"%s\").hasLabel(\"%s\").fields(\"%s\").order().by(\"%s\",Order.decr)",
			d.group, strings.Join(pkeys, ";"), d.edgeName, strings.Join(selectFields, ";"), sequenceConfig.TimestampField)

		request := aligraph.ReadRequest{
			QueryString: queryString,
		}
		resp, err := d.igraphClient.Read(&request)
		if err != nil {
			log.Println(err)
			return nil
		}

		results := []map[string]interface{}{}
		for _, resultData := range resp.Result {
			for _, data := range resultData.Data {
				properties := make(map[string]interface{}, len(data))

				for field, value := range data {
					if field == "label" ||
						field == fmt.Sprintf("%v_%v", d.primaryKeyField, sequenceConfig.EventField) ||
						(sequenceConfig.DeduplicationMethodNum == 2 && field == fmt.Sprintf("%v_%v", sequenceConfig.ItemIdField, sequenceConfig.TimestampField)) {
						continue
					}

					switch d.fieldTypeMap[field] {
					case constants.FS_DOUBLE, constants.FS_FLOAT:
						properties[field] = utils.ToFloat(value, -1024)
					case constants.FS_INT32, constants.FS_INT64:
						properties[field] = utils.ToInt(value, -1024)
					default:
						properties[field] = value
					}
				}
				if t, exist := sequencePlayTimeMap[utils.ToString(properties[sequenceConfig.EventField], "")]; exist {
					if utils.ToFloat(properties[sequenceConfig.PlayTimeField], 0.0) <= t {
						continue
					}
				}
				results = append(results, properties)
			}
		}

		return results
	}

	results := make([]map[string]interface{}, 0, len(userIds)*(len(events)+1)*50)
	var outmu sync.Mutex
	var wg sync.WaitGroup

	for _, userId := range userIds {
		wg.Add(1)
		go func(userId interface{}) {
			defer wg.Done()
			innerresult := fetchDataFunc(userId)
			outmu.Lock()
			results = append(results, innerresult...)
			outmu.Unlock()
		}(userId)
	}
	wg.Wait()

	return results, nil
}
