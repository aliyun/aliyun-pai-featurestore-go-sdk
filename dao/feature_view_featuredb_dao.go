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

const (
	FeatureDB_Protocal_Version_F    = byte('F')
	FeatureDB_IfNull_Flag_Version_1 = byte('1')
)

type FeatureViewFeatureDBDao struct {
	featureDBClient *http.Client
	database        string
	schema          string
	table           string
	address         string
	token           string
	fieldTypeMap    map[string]constants.FSType
	fields          []string
	signature       string
	primaryKeyField string
}

func NewFeatureViewFeatureDBDao(config DaoConfig) *FeatureViewFeatureDBDao {
	dao := FeatureViewFeatureDBDao{
		database:        config.FeatureDBDatabaseName,
		schema:          config.FeatureDBSchemaName,
		table:           config.FeatureDBTableName,
		fieldTypeMap:    config.FieldTypeMap,
		signature:       config.FeatureDBSignature,
		primaryKeyField: config.PrimaryKeyField,
		fields:          config.Fields,
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
	selectFieldsSet := make(map[string]struct{})
	for _, selectField := range selectFields {
		selectFieldsSet[selectField] = struct{}{}
	}

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
			defer response.Body.Close() // 确保关闭response.Body
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
			innerResult := make([]map[string]interface{}, 0, len(ks))
			innerReader := bytes.NewReader(nil)
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
					if len(dataBytes) < 2 {
						fmt.Println("key ", ks[keyStartIdx+i], " not exists")
						continue
					}
					innerReader.Reset(dataBytes)

					// 读取版本号
					var protocalVersion, ifNullFlagVersion uint8
					binary.Read(innerReader, binary.LittleEndian, &protocalVersion)
					binary.Read(innerReader, binary.LittleEndian, &ifNullFlagVersion)

					readFeatureDBFunc_F_1 := func() map[string]interface{} {
						properties := make(map[string]interface{})

						for _, field := range d.fields {
							var isNull uint8
							binary.Read(innerReader, binary.LittleEndian, &isNull)

							if isNull == 1 {
								// 跳过空值
								continue
							}
							if _, exists := selectFieldsSet[field]; exists {
								switch d.fieldTypeMap[field] {
								case constants.FS_DOUBLE:
									var float64Value float64
									binary.Read(innerReader, binary.LittleEndian, &float64Value)
									properties[field] = float64Value
								case constants.FS_FLOAT:
									var float32Value float32
									binary.Read(innerReader, binary.LittleEndian, &float32Value)
									properties[field] = float32Value
								case constants.FS_INT64:
									var int64Value int64
									binary.Read(innerReader, binary.LittleEndian, &int64Value)
									properties[field] = int64Value
								case constants.FS_INT32:
									var int32Value int32
									binary.Read(innerReader, binary.LittleEndian, &int32Value)
									properties[field] = int32Value
								case constants.FS_BOOLEAN:
									var booleanValue bool
									binary.Read(innerReader, binary.LittleEndian, &booleanValue)
									properties[field] = booleanValue
								default:
									var length uint32
									binary.Read(innerReader, binary.LittleEndian, &length)
									strBytes := make([]byte, length)
									binary.Read(innerReader, binary.LittleEndian, &strBytes)
									properties[field] = string(strBytes)
								}
							} else {
								var skipBytes int
								switch d.fieldTypeMap[field] {
								case constants.FS_DOUBLE:
									skipBytes = 8
								case constants.FS_FLOAT:
									skipBytes = 4
								case constants.FS_INT64:
									skipBytes = 8
								case constants.FS_INT32:
									skipBytes = 4
								case constants.FS_BOOLEAN:
									skipBytes = 1
								default:
									var length uint32
									binary.Read(innerReader, binary.LittleEndian, &length)
									skipBytes = int(length)
								}

								skipData := make([]byte, skipBytes)
								if _, err := io.ReadFull(innerReader, skipData); err != nil {
									panic(err)
								}
							}
						}
						properties[d.primaryKeyField] = ks[keyStartIdx+i]

						return properties
					}()

					if protocalVersion == FeatureDB_Protocal_Version_F && ifNullFlagVersion == FeatureDB_IfNull_Flag_Version_1 {
						innerResult = append(innerResult, readFeatureDBFunc_F_1)
					} else {
						panic(fmt.Sprintf("protocalVersion %v or ifNullFlagVersion %d is not supported\n", protocalVersion, ifNullFlagVersion))
					}
				}
				keyStartIdx += recordBlock.ValuesLength()
			}
			mu.Lock()
			result = append(result, innerResult...)
			mu.Unlock()
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
