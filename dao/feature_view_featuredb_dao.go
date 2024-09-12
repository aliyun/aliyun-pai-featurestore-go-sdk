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
	"strings"
	"sync"
	"time"

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

var readerPool sync.Pool

func init() {
	readerPool = sync.Pool{
		New: func() interface{} {
			return bytes.NewReader(nil)
		},
	}
}

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

	errChan := make(chan error, len(keys)/groupSize+1)
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
			requestBody := readerPool.Get().(*bytes.Reader)
			defer readerPool.Put(requestBody)
			requestBody.Reset(body)
			req, err := http.NewRequest("POST", fmt.Sprintf("%s/api/v1/tables/%s/%s/%s/batch_get_kv2?batch_size=%d&encoder=",
				d.address, d.database, d.schema, d.table, len(pkeys)), requestBody)
			if err != nil {
				errChan <- err
				return
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", d.token)
			req.Header.Set("Auth", d.signature)

			response, err := d.featureDBClient.Do(req)
			if err != nil {
				errChan <- err
				return
			}
			defer response.Body.Close() // 确保关闭response.Body
			// 检查状态码
			if response.StatusCode != http.StatusOK {
				bodyBytes, err := io.ReadAll(response.Body)
				if err != nil {
					errChan <- err
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
			innerReader := readerPool.Get().(*bytes.Reader)
			defer readerPool.Put(innerReader)
			for {
				buf, err := deserialize(reader)
				if err == io.EOF {
					break // End of stream
				}
				if err != nil {
					errChan <- err
					return
				}

				recordBlock := fdbserverfb.GetRootAsRecordBlock(buf, 0)

				for i := 0; i < recordBlock.ValuesLength(); i++ {
					value := new(fdbserverfb.UInt8ValueColumn)
					recordBlock.Values(value, i)
					dataBytes := value.ValueBytes()
					// key 不存在
					if len(dataBytes) < 2 {
						// fmt.Println("key ", ks[keyStartIdx+i], " not exists")
						continue
					}
					innerReader.Reset(dataBytes)

					// 读取版本号
					var protocalVersion, ifNullFlagVersion uint8
					binary.Read(innerReader, binary.LittleEndian, &protocalVersion)
					binary.Read(innerReader, binary.LittleEndian, &ifNullFlagVersion)

					readFeatureDBFunc_F_1 := func() (map[string]interface{}, error) {
						properties := make(map[string]interface{})

						for _, field := range d.fields {
							var isNull uint8
							if err := binary.Read(innerReader, binary.LittleEndian, &isNull); err != nil {
								if err == io.EOF {
									break
								}
								return nil, err
							}

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
								case constants.FS_STRING:
									var length uint32
									binary.Read(innerReader, binary.LittleEndian, &length)
									strBytes := make([]byte, length)
									binary.Read(innerReader, binary.LittleEndian, &strBytes)
									properties[field] = string(strBytes)
								case constants.FS_ARRAY_INT32:
									var length uint32
									binary.Read(innerReader, binary.LittleEndian, &length)
									arrayInt32Value := make([]int32, length)
									if length > 0 {
										binary.Read(innerReader, binary.LittleEndian, &arrayInt32Value)
									}
									properties[field] = arrayInt32Value
								case constants.FS_ARRAY_INT64:
									var length uint32
									binary.Read(innerReader, binary.LittleEndian, &length)
									arrayInt64Value := make([]int64, length)
									if length > 0 {
										binary.Read(innerReader, binary.LittleEndian, &arrayInt64Value)
									}
									properties[field] = arrayInt64Value
								case constants.FS_ARRAY_FLOAT:
									var length uint32
									binary.Read(innerReader, binary.LittleEndian, &length)
									arrayFloat32Value := make([]float32, length)
									if length > 0 {
										binary.Read(innerReader, binary.LittleEndian, &arrayFloat32Value)
									}
									properties[field] = arrayFloat32Value
								case constants.FS_ARRAY_DOUBLE:
									var length uint32
									binary.Read(innerReader, binary.LittleEndian, &length)
									arrayFloat64Value := make([]float64, length)
									if length > 0 {
										binary.Read(innerReader, binary.LittleEndian, &arrayFloat64Value)
									}
									properties[field] = arrayFloat64Value
								case constants.FS_ARRAY_STRING:
									var length uint32
									binary.Read(innerReader, binary.LittleEndian, &length)
									arrayStringValue := d.decodeStringArray(innerReader, length)
									properties[field] = arrayStringValue
								case constants.FS_ARRAY_ARRAY_FLOAT:
									var outerLength uint32
									binary.Read(innerReader, binary.LittleEndian, &outerLength)
									arrayOfArrayFloatValue := make([][]float32, outerLength)
									if outerLength > 0 {
										var totalElements uint32
										binary.Read(innerReader, binary.LittleEndian, &totalElements)
										if totalElements == 0 {
											for outerIdx := range arrayOfArrayFloatValue {
												arrayOfArrayFloatValue[outerIdx] = []float32{}
											}
										} else {
											innerArrayLens := make([]uint32, outerLength)
											binary.Read(innerReader, binary.LittleEndian, &innerArrayLens)
											innerValidElements := make([]float32, totalElements)
											binary.Read(innerReader, binary.LittleEndian, &innerValidElements)
											innerIndex := 0
											for outerIdx, innerLength := range innerArrayLens {
												arrayOfArrayFloatValue[outerIdx] = innerValidElements[innerIndex : innerIndex+int(innerLength)]
												innerIndex += int(innerLength)
											}
										}
									}
									properties[field] = arrayOfArrayFloatValue
								case constants.FS_MAP_INT32_INT32:
									var length uint32
									binary.Read(innerReader, binary.LittleEndian, &length)
									mapInt32Int32Value := make(map[int32]int32, length)
									if length > 0 {
										keys := make([]int32, length)
										binary.Read(innerReader, binary.LittleEndian, &keys)
										values := make([]int32, length)
										binary.Read(innerReader, binary.LittleEndian, &values)
										for idx, key := range keys {
											mapInt32Int32Value[key] = values[idx]
										}
									}
									properties[field] = mapInt32Int32Value
								case constants.FS_MAP_INT32_INT64:
									var length uint32
									binary.Read(innerReader, binary.LittleEndian, &length)
									mapInt32Int64Value := make(map[int32]int64, length)
									if length > 0 {
										keys := make([]int32, length)
										binary.Read(innerReader, binary.LittleEndian, &keys)
										values := make([]int64, length)
										binary.Read(innerReader, binary.LittleEndian, &values)
										for idx, key := range keys {
											mapInt32Int64Value[key] = values[idx]
										}
									}
									properties[field] = mapInt32Int64Value
								case constants.FS_MAP_INT32_FLOAT:
									var length uint32
									binary.Read(innerReader, binary.LittleEndian, &length)
									mapInt32FloatValue := make(map[int32]float32, length)
									if length > 0 {
										keys := make([]int32, length)
										binary.Read(innerReader, binary.LittleEndian, &keys)
										values := make([]float32, length)
										binary.Read(innerReader, binary.LittleEndian, &values)
										for idx, key := range keys {
											mapInt32FloatValue[key] = values[idx]
										}
									}
									properties[field] = mapInt32FloatValue
								case constants.FS_MAP_INT32_DOUBLE:
									var length uint32
									binary.Read(innerReader, binary.LittleEndian, &length)
									mapInt32DoubleValue := make(map[int32]float64, length)
									if length > 0 {
										keys := make([]int32, length)
										binary.Read(innerReader, binary.LittleEndian, &keys)
										values := make([]float64, length)
										binary.Read(innerReader, binary.LittleEndian, &values)
										for idx, key := range keys {
											mapInt32DoubleValue[key] = values[idx]
										}
									}
									properties[field] = mapInt32DoubleValue
								case constants.FS_MAP_INT32_STRING:
									var length uint32
									binary.Read(innerReader, binary.LittleEndian, &length)
									mapInt32StringValue := make(map[int32]string, length)
									if length > 0 {
										keys := make([]int32, length)
										binary.Read(innerReader, binary.LittleEndian, &keys)
										values := d.decodeStringArray(innerReader, length)
										for idx, key := range keys {
											mapInt32StringValue[key] = values[idx]
										}
									}
									properties[field] = mapInt32StringValue
								case constants.FS_MAP_INT64_INT32:
									var length uint32
									binary.Read(innerReader, binary.LittleEndian, &length)
									mapInt64Int32Value := make(map[int64]int32, length)
									if length > 0 {
										keys := make([]int64, length)
										binary.Read(innerReader, binary.LittleEndian, &keys)
										values := make([]int32, length)
										binary.Read(innerReader, binary.LittleEndian, &values)
										for idx, key := range keys {
											mapInt64Int32Value[key] = values[idx]
										}
									}
									properties[field] = mapInt64Int32Value
								case constants.FS_MAP_INT64_INT64:
									var length uint32
									binary.Read(innerReader, binary.LittleEndian, &length)
									mapInt64Int64Value := make(map[int64]int64, length)
									if length > 0 {
										keys := make([]int64, length)
										binary.Read(innerReader, binary.LittleEndian, &keys)
										values := make([]int64, length)
										binary.Read(innerReader, binary.LittleEndian, &values)
										for idx, key := range keys {
											mapInt64Int64Value[key] = values[idx]
										}
									}
									properties[field] = mapInt64Int64Value
								case constants.FS_MAP_INT64_FLOAT:
									var length uint32
									binary.Read(innerReader, binary.LittleEndian, &length)
									mapInt64FloatValue := make(map[int64]float32, length)
									if length > 0 {
										keys := make([]int64, length)
										binary.Read(innerReader, binary.LittleEndian, &keys)
										values := make([]float32, length)
										binary.Read(innerReader, binary.LittleEndian, &values)
										for idx, key := range keys {
											mapInt64FloatValue[key] = values[idx]
										}
									}
									properties[field] = mapInt64FloatValue
								case constants.FS_MAP_INT64_DOUBLE:
									var length uint32
									binary.Read(innerReader, binary.LittleEndian, &length)
									mapInt64DoubleValue := make(map[int64]float64, length)
									if length > 0 {
										keys := make([]int64, length)
										binary.Read(innerReader, binary.LittleEndian, &keys)
										values := make([]float64, length)
										binary.Read(innerReader, binary.LittleEndian, &values)
										for idx, key := range keys {
											mapInt64DoubleValue[key] = values[idx]
										}
									}
									properties[field] = mapInt64DoubleValue
								case constants.FS_MAP_INT64_STRING:
									var length uint32
									binary.Read(innerReader, binary.LittleEndian, &length)
									mapInt64StringValue := make(map[int64]string, length)
									if length > 0 {
										keys := make([]int64, length)
										binary.Read(innerReader, binary.LittleEndian, &keys)
										values := d.decodeStringArray(innerReader, length)
										for idx, key := range keys {
											mapInt64StringValue[key] = values[idx]
										}
									}
									properties[field] = mapInt64StringValue
								case constants.FS_MAP_STRING_INT32:
									var length uint32
									binary.Read(innerReader, binary.LittleEndian, &length)
									mapStringInt32Value := make(map[string]int32, length)
									if length > 0 {
										keys := d.decodeStringArray(innerReader, length)
										values := make([]int32, length)
										binary.Read(innerReader, binary.LittleEndian, &values)
										for idx, key := range keys {
											mapStringInt32Value[key] = values[idx]
										}
									}
									properties[field] = mapStringInt32Value
								case constants.FS_MAP_STRING_INT64:
									var length uint32
									binary.Read(innerReader, binary.LittleEndian, &length)
									mapStringInt64Value := make(map[string]int64, length)
									if length > 0 {
										keys := d.decodeStringArray(innerReader, length)
										values := make([]int64, length)
										binary.Read(innerReader, binary.LittleEndian, &values)
										for idx, key := range keys {
											mapStringInt64Value[key] = values[idx]
										}
									}
									properties[field] = mapStringInt64Value
								case constants.FS_MAP_STRING_FLOAT:
									var length uint32
									binary.Read(innerReader, binary.LittleEndian, &length)
									mapStringFloatValue := make(map[string]float32, length)
									if length > 0 {
										keys := d.decodeStringArray(innerReader, length)
										values := make([]float32, length)
										binary.Read(innerReader, binary.LittleEndian, &values)
										for idx, key := range keys {
											mapStringFloatValue[key] = values[idx]
										}
									}
									properties[field] = mapStringFloatValue
								case constants.FS_MAP_STRING_DOUBLE:
									var length uint32
									binary.Read(innerReader, binary.LittleEndian, &length)
									mapStringDoubleValue := make(map[string]float64, length)
									if length > 0 {
										keys := d.decodeStringArray(innerReader, length)
										values := make([]float64, length)
										binary.Read(innerReader, binary.LittleEndian, &values)
										for idx, key := range keys {
											mapStringDoubleValue[key] = values[idx]
										}
									}
									properties[field] = mapStringDoubleValue
								case constants.FS_MAP_STRING_STRING:
									var length uint32
									binary.Read(innerReader, binary.LittleEndian, &length)
									mapStringStringValue := make(map[string]string, length)
									if length > 0 {
										keys := d.decodeStringArray(innerReader, length)
										values := d.decodeStringArray(innerReader, length)
										for idx, key := range keys {
											mapStringStringValue[key] = values[idx]
										}
									}
									properties[field] = mapStringStringValue
								default:
									var length uint32
									binary.Read(innerReader, binary.LittleEndian, &length)
									strBytes := make([]byte, length)
									binary.Read(innerReader, binary.LittleEndian, &strBytes)
									properties[field] = string(strBytes)
								}
							} else {
								var skipBytes int = 0
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
								case constants.FS_STRING:
									var length uint32
									binary.Read(innerReader, binary.LittleEndian, &length)
									skipBytes = int(length)
								case constants.FS_ARRAY_INT32:
									var length uint32
									binary.Read(innerReader, binary.LittleEndian, &length)
									skipBytes = int(length) * 4
								case constants.FS_ARRAY_INT64:
									var length uint32
									binary.Read(innerReader, binary.LittleEndian, &length)
									skipBytes = int(length) * 8
								case constants.FS_ARRAY_FLOAT:
									var length uint32
									binary.Read(innerReader, binary.LittleEndian, &length)
									skipBytes = int(length) * 4
								case constants.FS_ARRAY_DOUBLE:
									var length uint32
									binary.Read(innerReader, binary.LittleEndian, &length)
									skipBytes = int(length) * 8
								case constants.FS_ARRAY_STRING:
									var length uint32
									binary.Read(innerReader, binary.LittleEndian, &length)
									skipBytes = d.getStringArrayCharLen(innerReader, length)
								case constants.FS_ARRAY_ARRAY_FLOAT:
									var outerLength uint32
									binary.Read(innerReader, binary.LittleEndian, &outerLength)
									if outerLength > 0 {
										var totalElements uint32
										binary.Read(innerReader, binary.LittleEndian, &totalElements)
										if totalElements > 0 {
											skipBytes = int(outerLength)*4 + int(totalElements)*4
										}
									}
								case constants.FS_MAP_INT32_INT32:
									var length uint32
									binary.Read(innerReader, binary.LittleEndian, &length)
									skipBytes = int(length) * (4 + 4)
								case constants.FS_MAP_INT32_INT64:
									var length uint32
									binary.Read(innerReader, binary.LittleEndian, &length)
									skipBytes = int(length) * (4 + 8)
								case constants.FS_MAP_INT32_FLOAT:
									var length uint32
									binary.Read(innerReader, binary.LittleEndian, &length)
									skipBytes = int(length) * (4 + 4)
								case constants.FS_MAP_INT32_DOUBLE:
									var length uint32
									binary.Read(innerReader, binary.LittleEndian, &length)
									skipBytes = int(length) * (4 + 8)
								case constants.FS_MAP_INT32_STRING:
									var length uint32
									binary.Read(innerReader, binary.LittleEndian, &length)
									innerReader.Seek(int64(length*4), io.SeekCurrent)
									skipBytes = d.getStringArrayCharLen(innerReader, length)
								case constants.FS_MAP_INT64_INT32:
									var length uint32
									binary.Read(innerReader, binary.LittleEndian, &length)
									skipBytes = int(length) * (8 + 4)
								case constants.FS_MAP_INT64_INT64:
									var length uint32
									binary.Read(innerReader, binary.LittleEndian, &length)
									skipBytes = int(length) * (8 + 8)
								case constants.FS_MAP_INT64_FLOAT:
									var length uint32
									binary.Read(innerReader, binary.LittleEndian, &length)
									skipBytes = int(length) * (8 + 4)
								case constants.FS_MAP_INT64_DOUBLE:
									var length uint32
									binary.Read(innerReader, binary.LittleEndian, &length)
									skipBytes = int(length) * (8 + 8)
								case constants.FS_MAP_INT64_STRING:
									var length uint32
									binary.Read(innerReader, binary.LittleEndian, &length)
									innerReader.Seek(int64(length*8), io.SeekCurrent)
									skipBytes = d.getStringArrayCharLen(innerReader, length)
								case constants.FS_MAP_STRING_INT32:
									var length uint32
									binary.Read(innerReader, binary.LittleEndian, &length)
									skipBytes = d.getStringArrayCharLen(innerReader, length) + int(length)*4
								case constants.FS_MAP_STRING_INT64:
									var length uint32
									binary.Read(innerReader, binary.LittleEndian, &length)
									skipBytes = d.getStringArrayCharLen(innerReader, length) + int(length)*8
								case constants.FS_MAP_STRING_FLOAT:
									var length uint32
									binary.Read(innerReader, binary.LittleEndian, &length)
									skipBytes = d.getStringArrayCharLen(innerReader, length) + int(length)*4
								case constants.FS_MAP_STRING_DOUBLE:
									var length uint32
									binary.Read(innerReader, binary.LittleEndian, &length)
									skipBytes = d.getStringArrayCharLen(innerReader, length) + int(length)*8
								case constants.FS_MAP_STRING_STRING:
									var length uint32
									binary.Read(innerReader, binary.LittleEndian, &length)
									keyLen := d.getStringArrayCharLen(innerReader, length)
									innerReader.Seek(int64(keyLen), io.SeekCurrent)
									skipBytes = d.getStringArrayCharLen(innerReader, length)
								default:
									var length uint32
									binary.Read(innerReader, binary.LittleEndian, &length)
									skipBytes = int(length)
								}

								if skipBytes > 0 {
									if _, err := innerReader.Seek(int64(skipBytes), io.SeekCurrent); err != nil {
										return nil, err
									}
								}
							}
						}
						properties[d.primaryKeyField] = ks[keyStartIdx+i]

						return properties, nil
					}

					if protocalVersion == FeatureDB_Protocal_Version_F && ifNullFlagVersion == FeatureDB_IfNull_Flag_Version_1 {
						readResult, err := readFeatureDBFunc_F_1()
						if err != nil {
							errChan <- err
							return
						}
						innerResult = append(innerResult, readResult)
					} else {
						errChan <- fmt.Errorf("FeatureDB read key %v error: protocalVersion %v or ifNullFlagVersion %d is not supported", ks[keyStartIdx+i], protocalVersion, ifNullFlagVersion)
						fmt.Printf("FeatureDB read key %v error: protocalVersion %v or ifNullFlagVersion %d is not supported", ks[keyStartIdx+i], protocalVersion, ifNullFlagVersion)
						return
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
	close(errChan)

	for err := range errChan {
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

func (d *FeatureViewFeatureDBDao) decodeStringArray(innerReader *bytes.Reader, length uint32) []string {
	arrayStringValue := make([]string, length)
	if length > 0 {
		offsets := make([]uint32, length+1)
		binary.Read(innerReader, binary.LittleEndian, &offsets)
		totalLength := offsets[length]

		stringData := make([]byte, totalLength)
		binary.Read(innerReader, binary.LittleEndian, &stringData)
		for strIdx := uint32(0); strIdx < length; strIdx++ {
			start := offsets[strIdx]
			end := offsets[strIdx+1]
			arrayStringValue[strIdx] = string(stringData[start:end])
		}
	}
	return arrayStringValue
}

func (d *FeatureViewFeatureDBDao) getStringArrayCharLen(innerReader *bytes.Reader, length uint32) int {
	if length > 0 {
		innerReader.Seek(int64(length*4), io.SeekCurrent)
		var totalLength uint32
		binary.Read(innerReader, binary.LittleEndian, &totalLength)
		return int(totalLength)
	}
	return 0
}

type FeatureDBBatchGetKKVRequest struct {
	PKs    []string `json:"pks"`
	Length int      `json:"length"`
}

func (d *FeatureViewFeatureDBDao) GetUserSequenceFeature(keys []interface{}, userIdField string, sequenceConfig api.FeatureViewSeqConfig, onlineConfig []*api.SeqConfig) ([]map[string]interface{}, error) {
	currTime := time.Now().Unix()
	sequencePlayTimeMap := makePlayTimeMap(sequenceConfig)

	errChan := make(chan error, len(keys)*len(onlineConfig))

	fetchDataFunc := func(seqEvent string, seqLen int, key interface{}) []*sequenceInfo {
		sequences := []*sequenceInfo{}

		events := strings.Split(seqEvent, "|")
		pks := []string{}
		for _, event := range events {
			pks = append(pks, fmt.Sprintf("%v\u001D%s", key, event))
		}
		request := FeatureDBBatchGetKKVRequest{
			PKs:    pks,
			Length: seqLen,
		}
		body, _ := json.Marshal(request)
		req, err := http.NewRequest("POST", fmt.Sprintf("%s/api/v1/tables/%s/%s/%s/batch_get_kkv",
			d.address, d.database, d.schema, d.table), bytes.NewReader(body))
		if err != nil {
			errChan <- err
			return nil
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", d.token)
		req.Header.Set("Auth", d.signature)

		response, err := d.featureDBClient.Do(req)
		if err != nil {
			errChan <- err
			return nil
		}
		defer response.Body.Close() // 确保关闭response.Body
		// 检查状态码
		if response.StatusCode != http.StatusOK {
			bodyBytes, err := io.ReadAll(response.Body)
			if err != nil {
				errChan <- err
				return nil
			}
			var bodyMap map[string]interface{}
			if err := json.Unmarshal(bodyBytes, &bodyMap); err == nil {
				if msg, found := bodyMap["message"]; found {
					log.Printf("StatusCode: %d, Response message: %s\n", response.StatusCode, msg)
				}
			}
			return nil
		}

		reader := bufio.NewReader(response.Body)
		for {
			buf, err := deserialize(reader)
			if err == io.EOF {
				break // End of stream
			}
			if err != nil {
				errChan <- err
				return nil
			}

			kkvRecordBlock := fdbserverfb.GetRootAsKKVRecordBlock(buf, 0)

			for i := 0; i < kkvRecordBlock.ValuesLength(); i++ {
				kkv := new(fdbserverfb.KKVData)
				kkvRecordBlock.Values(kkv, i)
				pk := string(kkv.Pk())
				userIdEvent := strings.Split(pk, "\u001D")
				if len(userIdEvent) != 2 {
					continue
				}
				var itemId string
				if sequenceConfig.DeduplicationMethodNum == 1 {
					itemId = string(kkv.Sk())
				} else if sequenceConfig.DeduplicationMethodNum == 2 {
					sk := string(kkv.Sk())
					itemIdTimestamp := strings.Split(sk, "\u001D")
					if len(itemIdTimestamp) != 2 {
						continue
					}
					itemId = itemIdTimestamp[0]
				} else {
					continue
				}

				seq := new(sequenceInfo)
				seq.event = userIdEvent[1]
				seq.itemId = itemId
				seq.timestamp = kkv.EventTimestamp()
				seq.playTime = kkv.PlayTime()

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

					// FeatureDB has processed the integration of online sequence features and offline sequence features
					// Here we put the results into onlineSequences

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

	close(errChan)

	for err := range errChan {
		if err != nil {
			return nil, err
		}
	}

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
