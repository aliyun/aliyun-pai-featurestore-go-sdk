package dao

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"

	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/api"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/constants"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/datasource/featuredb"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/datasource/featuredb/fdbserverfb"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/utils"
)

type FeatureViewFeatureDBDao struct {
	featureDBClient *http.Client
	database        string
	schema          string
	table           string
	address         string
	token           string
	fieldIndexMap   map[string]int
	fieldTypeMap    map[string]constants.FSType
	signature       string
	primaryKeyField string
}

func NewFeatureViewFeatureDBDao(config DaoConfig) *FeatureViewFeatureDBDao {
	dao := FeatureViewFeatureDBDao{
		database:        config.FeatureDBDatabaseName,
		schema:          config.FeatureDBSchemaName,
		table:           config.FeatureDBTableName,
		fieldIndexMap:   config.FieldIndexMap,
		fieldTypeMap:    config.FieldTypeMap,
		signature:       config.FeatureDBSignature,
		primaryKeyField: config.PrimaryKeyField,
	}
	client, err := featuredb.GetFeatureDBClient()
	if err != nil {
		return nil
	}

	dao.featureDBClient = client.Client

	dao.address = client.Address
	dao.token = client.Token

	return &dao
}

func (d *FeatureViewFeatureDBDao) GetFeatures(keys []interface{}, selectFields []string) ([]map[string]interface{}, error) {
	result := make([]map[string]interface{}, 0, len(keys))
	var wg sync.WaitGroup
	var mu sync.Mutex
	const groupSize = 200
	if d.signature == "" {
		return result, errors.New("FeatureStore DB username and password are not entered, please enter them by adding client.LoginFeatureStoreDB(username, password)")
	}
	if d.address == "" || d.token == "" {
		return result, errors.New("FeatureDB datasource has not been created")
	}

	for i := 0; i < len(keys); i += groupSize {
		end := i + groupSize
		if end > len(keys) {
			end = len(keys)
		}
		ks := keys[i:end]
		wg.Add(1)
		go func(ks []interface{}) {
			defer wg.Done()
			var pkeys []string
			for _, k := range ks {
				pkeys = append(pkeys, utils.ToString(k, ""))
			}
			body, _ := json.Marshal(map[string]any{"keys": pkeys})
			req, err := http.NewRequest("POST", fmt.Sprintf("%s/api/v1/tables/%s/%s/%s/batch_get_kv2?batch_size=%d&encoder=",
				d.address, d.database, d.schema, d.table, len(pkeys)), bytes.NewReader(body))
			if err != nil {
				log.Println(err)
				return
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", d.token)
			req.Header.Set("Auth", d.signature)

			response, err := d.featureDBClient.Do(req)
			if err != nil {
				log.Println(err)
				return
			}
			// 检查状态码
			if response.StatusCode != http.StatusOK {
				bodyBytes, err := io.ReadAll(response.Body)
				if err != nil {
					log.Println(err)
					return
				}

				var bodyMap map[string]interface{}
				if err := json.Unmarshal(bodyBytes, &bodyMap); err == nil {
					if msg, found := bodyMap["message"]; found {
						log.Printf("StatusCode: %d, Response message: %s\n", response.StatusCode, msg)
					}
				}
				return
			}

			reader := bufio.NewReader(response.Body)
			keyStartIdx := 0
			for {
				buf, err := deserialize(reader)
				if err == io.EOF {
					break // End of stream
				}
				if err != nil {
					fmt.Println(err)
				}

				recordBlock := fdbserverfb.GetRootAsRecordBlock(buf, 0)

				for i := 0; i < recordBlock.ValuesLength(); i++ {
					value := new(fdbserverfb.UInt8ValueColumn)
					recordBlock.Values(value, i)
					dataBytes := value.ValueBytes()
					// key 不存在
					if len(dataBytes) == 0 {
						fmt.Println("key ", ks[keyStartIdx+i], " not exists")
						continue
					}
					separator := []byte("\u001E")
					fieldValues := bytes.Split(dataBytes, separator)

					properties := make(map[string]interface{}, len(fieldValues)+1)

					for _, field := range selectFields {
						fieldIdx := d.fieldIndexMap[field]
						reader := bytes.NewReader(fieldValues[fieldIdx])
						switch d.fieldTypeMap[field] {
						case constants.FS_DOUBLE:
							var float64Value float64
							binary.Read(reader, binary.LittleEndian, &float64Value)
							properties[field] = float64Value
						case constants.FS_FLOAT:
							var float32Value float32
							binary.Read(reader, binary.LittleEndian, &float32Value)
							properties[field] = float32Value
						case constants.FS_INT64:
							var int64Value int64
							binary.Read(reader, binary.LittleEndian, &int64Value)
							properties[field] = int64Value
						case constants.FS_INT32:
							var int32Value int32
							binary.Read(reader, binary.LittleEndian, &int32Value)
							properties[field] = int32Value
						case constants.FS_BOOLEAN:
							var booleanValue bool
							binary.Read(reader, binary.LittleEndian, &booleanValue)
							properties[field] = booleanValue
						default:
							properties[field] = string(fieldValues[fieldIdx])
						}
					}
					properties[d.primaryKeyField] = ks[keyStartIdx+i]
					mu.Lock()
					result = append(result, properties)
					mu.Unlock()
				}
				keyStartIdx += recordBlock.ValuesLength()
			}
			response.Body.Close()
		}(ks)

	}
	wg.Wait()

	return result, nil
}

func (d *FeatureViewFeatureDBDao) GetUserSequenceFeature(keys []interface{}, userIdField string, sequenceConfig api.FeatureViewSeqConfig, onlineConfig []*api.SeqConfig) ([]map[string]interface{}, error) {
	results := make([]map[string]interface{}, 0, len(keys))

	return results, nil
}

func deserialize(r io.Reader) ([]byte, error) {
	var length uint32
	err := binary.Read(r, binary.LittleEndian, &length)
	if err != nil {
		return nil, err
	}
	data := make([]byte, length)
	_, err = io.ReadFull(r, data)
	if err != nil {
		return nil, err
	}

	return data, nil
}
