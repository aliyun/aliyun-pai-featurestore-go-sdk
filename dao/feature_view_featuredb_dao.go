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

	"github.com/aliyun/aliyun-odps-go-sdk/arrow/array"
	"github.com/aliyun/aliyun-odps-go-sdk/arrow/ipc"
	"github.com/aliyun/aliyun-odps-go-sdk/arrow/memory"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/api"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/constants"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/datasource/featuredb"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/datasource/featuredb/fdbserverfb"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/utils"
	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
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
	UnimplementedFeatureViewDao
	featureDBClient *featuredb.FeatureDBClient
	database        string
	schema          string
	table           string
	fieldTypeMap    map[string]constants.FSType
	fields          []string
	signature       string
	primaryKeyField string
}

func CntSkipBytes(innerReader *bytes.Reader, fieldType constants.FSType) int {
	var skipBytes int = 0
	switch fieldType {
	case constants.FS_INT32:
		skipBytes = 4
	case constants.FS_INT64:
		skipBytes = 8
	case constants.FS_FLOAT:
		skipBytes = 4
	case constants.FS_DOUBLE:
		skipBytes = 8
	case constants.FS_STRING:
		var length uint32
		binary.Read(innerReader, binary.LittleEndian, &length)
		skipBytes = int(length)
	case constants.FS_BOOLEAN:
		skipBytes = 1
	default:
		var length uint32
		binary.Read(innerReader, binary.LittleEndian, &length)
		skipBytes = int(length)
	}
	return skipBytes
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

	dao.featureDBClient = client

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
	groupSize := len(keys) / 4
	groupSize = max(groupSize, 200)
	groupSize = min(groupSize, 500)
	if d.signature == "" {
		return result, errors.New("FeatureStore DB username and password are not entered, please enter them by adding client.LoginFeatureStoreDB(username, password)")
	}
	if d.featureDBClient.GetCurrentAddress(false) == "" || d.featureDBClient.Token == "" {
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
			url := fmt.Sprintf("%s/api/v1/tables/%s/%s/%s/batch_get_kv2?batch_size=%d&encoder=", d.featureDBClient.GetCurrentAddress(false), d.database, d.schema, d.table, len(pkeys))
			requestBody := readerPool.Get().(*bytes.Reader)
			defer readerPool.Put(requestBody)
			requestBody.Reset(body)
			req, err := http.NewRequest("POST", url, requestBody)
			if err != nil {
				errChan <- err
				return
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", d.featureDBClient.Token)
			req.Header.Set("Auth", d.signature)

			response, err := d.featureDBClient.Client.Do(req)
			if err != nil {
				url = fmt.Sprintf("%s/api/v1/tables/%s/%s/%s/batch_get_kv2?batch_size=%d&encoder=", d.featureDBClient.GetCurrentAddress(true), d.database, d.schema, d.table, len(pkeys))
				requestBody.Reset(body)
				req, err = http.NewRequest("POST", url, requestBody)
				if err != nil {
					errChan <- err
					return
				}
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Authorization", d.featureDBClient.Token)
				req.Header.Set("Auth", d.signature)
				response, err = d.featureDBClient.Client.Do(req)
				if err != nil {
					errChan <- err
					return
				}
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
					dataCursor := utils.NewByteCursor(dataBytes)

					// 读取版本号
					protocalVersion := dataCursor.ReadUint8()
					ifNullFlagVersion := dataCursor.ReadUint8()

					readFeatureDBFunc_F_1 := func() (map[string]interface{}, error) {
						properties := make(map[string]interface{})

						for _, field := range d.fields {
							isNull, ok := dataCursor.TryReadUint8()
							if !ok {
								// EOF
								break
							}

							if isNull == 1 {
								// 跳过空值
								continue
							}
							fieldType := d.fieldTypeMap[field]
							_, isSelected := selectFieldsSet[field]
							if isSelected {
								switch fieldType {
								case constants.FS_DOUBLE:
									properties[field] = dataCursor.ReadFloat64()
								case constants.FS_FLOAT:
									properties[field] = dataCursor.ReadFloat32()
								case constants.FS_INT64:
									properties[field] = dataCursor.ReadInt64()
								case constants.FS_INT32:
									properties[field] = dataCursor.ReadInt32()
								case constants.FS_BOOLEAN:
									properties[field] = dataCursor.ReadBool()
								case constants.FS_STRING:
									properties[field] = dataCursor.ReadString()
								case constants.FS_TIMESTAMP:
									properties[field] = time.UnixMilli(dataCursor.ReadInt64())
								case constants.FS_ARRAY_INT32:
									properties[field] = dataCursor.ReadInt32Slice(dataCursor.ReadUint32())
								case constants.FS_ARRAY_INT64:
									properties[field] = dataCursor.ReadInt64Slice(dataCursor.ReadUint32())
								case constants.FS_ARRAY_FLOAT:
									properties[field] = dataCursor.ReadFloat32Slice(dataCursor.ReadUint32())
								case constants.FS_ARRAY_DOUBLE:
									properties[field] = dataCursor.ReadFloat64Slice(dataCursor.ReadUint32())
								case constants.FS_ARRAY_STRING:
									properties[field] = dataCursor.ReadStringArray(dataCursor.ReadUint32())
								case constants.FS_ARRAY_ARRAY_FLOAT:
									outerLength := dataCursor.ReadUint32()
									if outerLength == 0 {
										properties[field] = [][]float32{}
									} else {
										totalElements := dataCursor.ReadUint32()
										if totalElements == 0 {
											arr := make([][]float32, outerLength)
											for k := uint32(0); k < outerLength; k++ {
												arr[k] = []float32{}
											}
											properties[field] = arr
										} else {
											innerArrayLens := dataCursor.ReadUint32Slice(outerLength)
											innerValidElements := dataCursor.ReadFloat32Slice(totalElements)
											result := make([][]float32, outerLength)
											innerIndex := 0
											for outerIdx, innerLength := range innerArrayLens {
												result[outerIdx] = innerValidElements[innerIndex : innerIndex+int(innerLength)]
												innerIndex += int(innerLength)
											}
											properties[field] = result
										}
									}
								case constants.FS_MAP_INT32_INT32:
									length := dataCursor.ReadUint32()
									mapInt32Int32Value := make(map[int32]int32, length)
									if length > 0 {
										keys := dataCursor.ReadInt32Slice(length)
										values := dataCursor.ReadInt32Slice(length)
										for k := uint32(0); k < length; k++ {
											mapInt32Int32Value[keys[k]] = values[k]
										}
									}
									properties[field] = mapInt32Int32Value
								case constants.FS_MAP_INT32_INT64:
									length := dataCursor.ReadUint32()
									mapInt32Int64Value := make(map[int32]int64, length)
									if length > 0 {
										keys := dataCursor.ReadInt32Slice(length)
										values := dataCursor.ReadInt64Slice(length)
										for k := uint32(0); k < length; k++ {
											mapInt32Int64Value[keys[k]] = values[k]
										}
									}
									properties[field] = mapInt32Int64Value
								case constants.FS_MAP_INT32_FLOAT:
									length := dataCursor.ReadUint32()
									mapInt32FloatValue := make(map[int32]float32, length)
									if length > 0 {
										keys := dataCursor.ReadInt32Slice(length)
										values := dataCursor.ReadFloat32Slice(length)
										for k := uint32(0); k < length; k++ {
											mapInt32FloatValue[keys[k]] = values[k]
										}
									}
									properties[field] = mapInt32FloatValue
								case constants.FS_MAP_INT32_DOUBLE:
									length := dataCursor.ReadUint32()
									mapInt32DoubleValue := make(map[int32]float64, length)
									if length > 0 {
										keys := dataCursor.ReadInt32Slice(length)
										values := dataCursor.ReadFloat64Slice(length)
										for k := uint32(0); k < length; k++ {
											mapInt32DoubleValue[keys[k]] = values[k]
										}
									}
									properties[field] = mapInt32DoubleValue
								case constants.FS_MAP_INT32_STRING:
									length := dataCursor.ReadUint32()
									mapInt32StringValue := make(map[int32]string, length)
									if length > 0 {
										keys := dataCursor.ReadInt32Slice(length)
										values := dataCursor.ReadStringArray(length)
										for k := uint32(0); k < length; k++ {
											mapInt32StringValue[keys[k]] = values[k]
										}
									}
									properties[field] = mapInt32StringValue
								case constants.FS_MAP_INT64_INT32:
									length := dataCursor.ReadUint32()
									mapInt64Int32Value := make(map[int64]int32, length)
									if length > 0 {
										keys := dataCursor.ReadInt64Slice(length)
										values := dataCursor.ReadInt32Slice(length)
										for k := uint32(0); k < length; k++ {
											mapInt64Int32Value[keys[k]] = values[k]
										}
									}
									properties[field] = mapInt64Int32Value
								case constants.FS_MAP_INT64_INT64:
									length := dataCursor.ReadUint32()
									mapInt64Int64Value := make(map[int64]int64, length)
									if length > 0 {
										keys := dataCursor.ReadInt64Slice(length)
										values := dataCursor.ReadInt64Slice(length)
										for k := uint32(0); k < length; k++ {
											mapInt64Int64Value[keys[k]] = values[k]
										}
									}
									properties[field] = mapInt64Int64Value
								case constants.FS_MAP_INT64_FLOAT:
									length := dataCursor.ReadUint32()
									mapInt64FloatValue := make(map[int64]float32, length)
									if length > 0 {
										keys := dataCursor.ReadInt64Slice(length)
										values := dataCursor.ReadFloat32Slice(length)
										for k := uint32(0); k < length; k++ {
											mapInt64FloatValue[keys[k]] = values[k]
										}
									}
									properties[field] = mapInt64FloatValue
								case constants.FS_MAP_INT64_DOUBLE:
									length := dataCursor.ReadUint32()
									mapInt64DoubleValue := make(map[int64]float64, length)
									if length > 0 {
										keys := dataCursor.ReadInt64Slice(length)
										values := dataCursor.ReadFloat64Slice(length)
										for k := uint32(0); k < length; k++ {
											mapInt64DoubleValue[keys[k]] = values[k]
										}
									}
									properties[field] = mapInt64DoubleValue
								case constants.FS_MAP_INT64_STRING:
									length := dataCursor.ReadUint32()
									mapInt64StringValue := make(map[int64]string, length)
									if length > 0 {
										keys := dataCursor.ReadInt64Slice(length)
										values := dataCursor.ReadStringArray(length)
										for k := uint32(0); k < length; k++ {
											mapInt64StringValue[keys[k]] = values[k]
										}
									}
									properties[field] = mapInt64StringValue
								case constants.FS_MAP_STRING_INT32:
									length := dataCursor.ReadUint32()
									mapStringInt32Value := make(map[string]int32, length)
									if length > 0 {
										keys := dataCursor.ReadStringArray(length)
										values := dataCursor.ReadInt32Slice(length)
										for k := uint32(0); k < length; k++ {
											mapStringInt32Value[keys[k]] = values[k]
										}
									}
									properties[field] = mapStringInt32Value
								case constants.FS_MAP_STRING_INT64:
									length := dataCursor.ReadUint32()
									mapStringInt64Value := make(map[string]int64, length)
									if length > 0 {
										keys := dataCursor.ReadStringArray(length)
										values := dataCursor.ReadInt64Slice(length)
										for k := uint32(0); k < length; k++ {
											mapStringInt64Value[keys[k]] = values[k]
										}
									}
									properties[field] = mapStringInt64Value
								case constants.FS_MAP_STRING_FLOAT:
									length := dataCursor.ReadUint32()
									mapStringFloatValue := make(map[string]float32, length)
									if length > 0 {
										keys := dataCursor.ReadStringArray(length)
										values := dataCursor.ReadFloat32Slice(length)
										for k := uint32(0); k < length; k++ {
											mapStringFloatValue[keys[k]] = values[k]
										}
									}
									properties[field] = mapStringFloatValue
								case constants.FS_MAP_STRING_DOUBLE:
									length := dataCursor.ReadUint32()
									mapStringDoubleValue := make(map[string]float64, length)
									if length > 0 {
										keys := dataCursor.ReadStringArray(length)
										values := dataCursor.ReadFloat64Slice(length)
										for k := uint32(0); k < length; k++ {
											mapStringDoubleValue[keys[k]] = values[k]
										}
									}
									properties[field] = mapStringDoubleValue
								case constants.FS_MAP_STRING_STRING:
									length := dataCursor.ReadUint32()
									mapStringStringValue := make(map[string]string, length)
									if length > 0 {
										keys := dataCursor.ReadStringArray(length)
										values := dataCursor.ReadStringArray(length)
										for k := uint32(0); k < length; k++ {
											mapStringStringValue[keys[k]] = values[k]
										}
									}
									properties[field] = mapStringStringValue
								default:
									properties[field] = dataCursor.ReadStringArray(dataCursor.ReadUint32())
								}
							} else {
								switch fieldType {
								case constants.FS_DOUBLE:
									dataCursor.Skip(8)
								case constants.FS_FLOAT:
									dataCursor.Skip(4)
								case constants.FS_INT64:
									dataCursor.Skip(8)
								case constants.FS_INT32:
									dataCursor.Skip(4)
								case constants.FS_BOOLEAN:
									dataCursor.Skip(1)
								case constants.FS_STRING:
									dataCursor.Skip(int(dataCursor.ReadUint32()))
								case constants.FS_TIMESTAMP:
									dataCursor.Skip(8)
								case constants.FS_ARRAY_INT32:
									dataCursor.Skip(int(dataCursor.ReadUint32()) * 4)
								case constants.FS_ARRAY_INT64:
									dataCursor.Skip(int(dataCursor.ReadUint32()) * 8)
								case constants.FS_ARRAY_FLOAT:
									dataCursor.Skip(int(dataCursor.ReadUint32()) * 4)
								case constants.FS_ARRAY_DOUBLE:
									dataCursor.Skip(int(dataCursor.ReadUint32()) * 8)
								case constants.FS_ARRAY_STRING:
									dataCursor.SkipStringArray(dataCursor.ReadUint32())
								case constants.FS_ARRAY_ARRAY_FLOAT:
									outerLength := dataCursor.ReadUint32()
									if outerLength > 0 {
										totalElements := dataCursor.ReadUint32()
										if totalElements > 0 {
											dataCursor.Skip(int(outerLength*4 + totalElements*4))
										}
									}
								case constants.FS_MAP_INT32_INT32:
									dataCursor.Skip(int(dataCursor.ReadUint32()) * (4 + 4))
								case constants.FS_MAP_INT32_INT64:
									dataCursor.Skip(int(dataCursor.ReadUint32()) * (4 + 8))
								case constants.FS_MAP_INT32_FLOAT:
									dataCursor.Skip(int(dataCursor.ReadUint32()) * (4 + 4))
								case constants.FS_MAP_INT32_DOUBLE:
									dataCursor.Skip(int(dataCursor.ReadUint32()) * (4 + 8))
								case constants.FS_MAP_INT32_STRING:
									length := dataCursor.ReadUint32()
									dataCursor.Skip(int(length * 4))
									dataCursor.SkipStringArray(length)
								case constants.FS_MAP_INT64_INT32:
									dataCursor.Skip(int(dataCursor.ReadUint32()) * (8 + 4))
								case constants.FS_MAP_INT64_INT64:
									dataCursor.Skip(int(dataCursor.ReadUint32()) * (8 + 8))
								case constants.FS_MAP_INT64_FLOAT:
									dataCursor.Skip(int(dataCursor.ReadUint32()) * (8 + 4))
								case constants.FS_MAP_INT64_DOUBLE:
									dataCursor.Skip(int(dataCursor.ReadUint32()) * (8 + 8))
								case constants.FS_MAP_INT64_STRING:
									length := dataCursor.ReadUint32()
									dataCursor.Skip(int(length * 8))
									dataCursor.SkipStringArray(length)
								case constants.FS_MAP_STRING_INT32:
									length := dataCursor.ReadUint32()
									dataCursor.SkipStringArray(length)
									dataCursor.Skip(int(length * 4))
								case constants.FS_MAP_STRING_INT64:
									length := dataCursor.ReadUint32()
									dataCursor.SkipStringArray(length)
									dataCursor.Skip(int(length * 8))
								case constants.FS_MAP_STRING_FLOAT:
									length := dataCursor.ReadUint32()
									dataCursor.SkipStringArray(length)
									dataCursor.Skip(int(length * 4))
								case constants.FS_MAP_STRING_DOUBLE:
									length := dataCursor.ReadUint32()
									dataCursor.SkipStringArray(length)
									dataCursor.Skip(int(length * 8))
								case constants.FS_MAP_STRING_STRING:
									length := dataCursor.ReadUint32()
									dataCursor.SkipStringArray(length)
									dataCursor.SkipStringArray(length)
								default:
									dataCursor.Skip(int(dataCursor.ReadUint32()))
								}
							}
							if dataCursor.Err != nil {
								return nil, dataCursor.Err
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

type FeatureDBBatchGetKKVRequest struct {
	PKs       []string `json:"pks"`
	Length    int      `json:"length"`
	WithValue bool     `json:"with_value"`
}

func (d *FeatureViewFeatureDBDao) GetUserSequenceFeature(keys []interface{}, userIdField string, sequenceConfig api.FeatureViewSeqConfig, onlineConfig []*api.SeqConfig) ([]map[string]interface{}, error) {
	currTime := time.Now().Unix()
	sequencePlayTimeMap := makePlayTimeMap(sequenceConfig.PlayTimeFilter)

	seqConfigsMap := make(map[string][]*api.SeqConfig)                  // seqEvent -> seqConfigs
	seqConfigsBehaviorFieldsMap := make(map[string]map[string]struct{}) // seqEvent -> behaviorFields
	maxSeqLenMap := make(map[string]int)                                // 每个 seqEvent 最大的 seqLen

	withValue := false
	for _, seqConfig := range onlineConfig {
		mapKey := seqConfig.SeqEvent
		seqConfigsMap[mapKey] = append(seqConfigsMap[mapKey], seqConfig)
		if seqConfig.SeqLen > maxSeqLenMap[mapKey] {
			maxSeqLenMap[mapKey] = seqConfig.SeqLen
		}

		if _, exists := seqConfigsBehaviorFieldsMap[mapKey]; !exists {
			seqConfigsBehaviorFieldsMap[mapKey] = make(map[string]struct{})
		}
		for _, field := range seqConfig.OnlineBehaviorTableFields {
			seqConfigsBehaviorFieldsMap[mapKey][field] = struct{}{}
		}

		if len(seqConfig.OnlineBehaviorTableFields) > 0 {
			withValue = true
		}
	}

	errChan := make(chan error, len(keys)*len(onlineConfig))

	fetchDataFunc := func(seqEvent string, seqLen int, key interface{}, selectBehaviorFieldsSet map[string]struct{}) []*sequenceInfo {
		sequences := []*sequenceInfo{}

		events := strings.Split(seqEvent, "|")
		pks := []string{}
		for _, event := range events {
			pks = append(pks, fmt.Sprintf("%v\u001D%s", key, event))
		}
		request := FeatureDBBatchGetKKVRequest{
			PKs:       pks,
			Length:    seqLen,
			WithValue: withValue,
		}
		body, _ := json.Marshal(request)
		url := fmt.Sprintf("%s/api/v1/tables/%s/%s/%s/batch_get_kkv", d.featureDBClient.GetCurrentAddress(false), d.database, d.schema, d.table)
		req, err := http.NewRequest("POST", url, bytes.NewReader(body))
		if err != nil {
			errChan <- err
			return nil
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", d.featureDBClient.Token)
		req.Header.Set("Auth", d.signature)

		response, err := d.featureDBClient.Client.Do(req)
		if err != nil {
			url = fmt.Sprintf("%s/api/v1/tables/%s/%s/%s/batch_get_kkv", d.featureDBClient.GetCurrentAddress(true), d.database, d.schema, d.table)
			req, err = http.NewRequest("POST", url, bytes.NewReader(body))
			if err != nil {
				errChan <- err
				return nil
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", d.featureDBClient.Token)
			req.Header.Set("Auth", d.signature)
			response, err = d.featureDBClient.Client.Do(req)

			if err != nil {
				errChan <- err
				return nil
			}
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
		innerReader := readerPool.Get().(*bytes.Reader)
		defer readerPool.Put(innerReader)
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

				seq.onlineBehaviourTableFieldsMap = make(map[string]string)

				if seq.event == "" || seq.itemId == "" {
					continue
				}
				if t, exist := sequencePlayTimeMap[seq.event]; exist {
					if seq.playTime <= t {
						continue
					}
				}
				dataBytes := kkv.ValueBytes()
				if len(dataBytes) < 2 {
					sequences = append(sequences, seq)
					continue
				}
				innerReader.Reset(dataBytes)
				// 读取版本号
				var protocalVersion, ifNullFlagVersion uint8
				binary.Read(innerReader, binary.LittleEndian, &protocalVersion)
				binary.Read(innerReader, binary.LittleEndian, &ifNullFlagVersion)
				readFeatureDBFunc_F_1 := func() (map[string]string, error) {
					properties := make(map[string]string)

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

						if _, exists := selectBehaviorFieldsSet[field]; exists {
							switch d.fieldTypeMap[field] {
							case constants.FS_INT32:
								var int32Value int32
								binary.Read(innerReader, binary.LittleEndian, &int32Value)
								properties[field] = fmt.Sprintf("%d", int32Value)
							case constants.FS_INT64:
								var int64Value int64
								binary.Read(innerReader, binary.LittleEndian, &int64Value)
								properties[field] = fmt.Sprintf("%d", int64Value)
							case constants.FS_FLOAT:
								var float32Value float32
								binary.Read(innerReader, binary.LittleEndian, &float32Value)
								properties[field] = fmt.Sprintf("%v", float32Value)
							case constants.FS_DOUBLE:
								var float64Value float64
								binary.Read(innerReader, binary.LittleEndian, &float64Value)
								properties[field] = fmt.Sprintf("%v", float64Value)
							case constants.FS_STRING:
								var length uint32
								binary.Read(innerReader, binary.LittleEndian, &length)
								strBytes := make([]byte, length)
								binary.Read(innerReader, binary.LittleEndian, &strBytes)
								properties[field] = string(strBytes)
							case constants.FS_BOOLEAN:
								var boolValue bool
								binary.Read(innerReader, binary.LittleEndian, &boolValue)
								properties[field] = fmt.Sprintf("%v", boolValue)
							default:
								var length uint32
								binary.Read(innerReader, binary.LittleEndian, &length)
								strBytes := make([]byte, length)
								binary.Read(innerReader, binary.LittleEndian, &strBytes)
								properties[field] = string(strBytes)
							}
						} else {
							skipBytes := CntSkipBytes(innerReader, d.fieldTypeMap[field])
							if skipBytes > 0 {
								if _, err := innerReader.Seek(int64(skipBytes), io.SeekCurrent); err != nil {
									return nil, err
								}
							}
						}
					}
					return properties, nil
				}

				if protocalVersion == FeatureDB_Protocal_Version_F && ifNullFlagVersion == FeatureDB_IfNull_Flag_Version_1 {
					readResult, err := readFeatureDBFunc_F_1()
					if err != nil {
						errChan <- err
						return nil
					}
					seq.onlineBehaviourTableFieldsMap = readResult
				} else {
					errChan <- fmt.Errorf("unsupported protocal version: %d, ifNullFlagVersion: %d", protocalVersion, ifNullFlagVersion)
					continue
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
			for seqEvent, seqConfigs := range seqConfigsMap {
				if len(seqConfigs) == 0 {
					continue
				}
				seqConfigsBehaviorFields := seqConfigsBehaviorFieldsMap[seqEvent]
				maxLen := maxSeqLenMap[seqEvent]
				eventWg.Add(1)
				go func(seqEvent string, seqConfigs []*api.SeqConfig, maxLen int, seqConfigsBehaviorFields map[string]struct{}) {
					defer eventWg.Done()

					// FeatureDB has processed the integration of online sequence features and offline sequence features
					// Here we put the results into onlineSequences

					onlineSequences := fetchDataFunc(seqEvent, maxLen, key, seqConfigsBehaviorFields)

					for _, seqConfig := range seqConfigs {
						var truncatedSequences []*sequenceInfo
						if seqConfig.SeqLen >= len(onlineSequences) {
							truncatedSequences = onlineSequences
						} else {
							truncatedSequences = onlineSequences[:seqConfig.SeqLen]
						}

						subproperties := makeSequenceFeatures4FeatureDB(truncatedSequences, seqConfig, sequenceConfig, currTime)
						mu.Lock()
						for k, value := range subproperties {
							properties[k] = value
						}
						mu.Unlock()
					}
				}(seqEvent, seqConfigs, maxLen, seqConfigsBehaviorFields)
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

func (d *FeatureViewFeatureDBDao) GetUserAggregatedSequenceFeature(keys []interface{}, userIdField string, sequenceConfig api.FeatureViewSeqConfig, onlineConfig []*api.SeqConfig) (map[string]interface{}, error) {
	currTime := time.Now().Unix()
	sequencePlayTimeMap := makePlayTimeMap(sequenceConfig.PlayTimeFilter)

	seqConfigsMap := make(map[string][]*api.SeqConfig)                  // seqEvent -> seqConfigs
	seqConfigsBehaviorFieldsMap := make(map[string]map[string]struct{}) // seqEvent -> behaviorFields
	maxSeqLenMap := make(map[string]int)                                // 每个 seqEvent 最大的 seqLen

	withValue := false
	for _, seqConfig := range onlineConfig {
		mapKey := seqConfig.SeqEvent
		seqConfigsMap[mapKey] = append(seqConfigsMap[mapKey], seqConfig)
		if seqConfig.SeqLen > maxSeqLenMap[mapKey] {
			maxSeqLenMap[mapKey] = seqConfig.SeqLen
		}

		if _, exists := seqConfigsBehaviorFieldsMap[mapKey]; !exists {
			seqConfigsBehaviorFieldsMap[mapKey] = make(map[string]struct{})
		}
		for _, field := range seqConfig.OnlineBehaviorTableFields {
			seqConfigsBehaviorFieldsMap[mapKey][field] = struct{}{}
		}

		if len(seqConfig.OnlineBehaviorTableFields) > 0 {
			withValue = true
		}
	}

	errChan := make(chan error, len(keys)*len(onlineConfig))

	fetchDataFunc := func(seqEvent string, seqLen int, keys []interface{}, selectBehaviorFieldsSet map[string]struct{}) []*sequenceInfo {
		sequences := []*sequenceInfo{}

		events := strings.Split(seqEvent, "|")
		pks := []string{}
		for _, event := range events {
			for _, key := range keys {
				pks = append(pks, fmt.Sprintf("%v\u001D%s", key, event))
			}
		}
		request := FeatureDBBatchGetKKVRequest{
			PKs:       pks,
			Length:    seqLen,
			WithValue: withValue,
		}
		body, _ := json.Marshal(request)
		url := fmt.Sprintf("%s/api/v1/tables/%s/%s/%s/batch_get_kkv", d.featureDBClient.GetCurrentAddress(false), d.database, d.schema, d.table)
		req, err := http.NewRequest("POST", url, bytes.NewReader(body))
		if err != nil {
			errChan <- err
			return nil
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", d.featureDBClient.Token)
		req.Header.Set("Auth", d.signature)

		response, err := d.featureDBClient.Client.Do(req)
		if err != nil {
			url = fmt.Sprintf("%s/api/v1/tables/%s/%s/%s/batch_get_kkv", d.featureDBClient.GetCurrentAddress(true), d.database, d.schema, d.table)
			req, err = http.NewRequest("POST", url, bytes.NewReader(body))
			if err != nil {
				errChan <- err
				return nil
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", d.featureDBClient.Token)
			req.Header.Set("Auth", d.signature)
			response, err = d.featureDBClient.Client.Do(req)

			if err != nil {
				errChan <- err
				return nil
			}
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
		innerReader := readerPool.Get().(*bytes.Reader)
		defer readerPool.Put(innerReader)
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

				seq.onlineBehaviourTableFieldsMap = make(map[string]string)

				if seq.event == "" || seq.itemId == "" {
					continue
				}
				if t, exist := sequencePlayTimeMap[seq.event]; exist {
					if seq.playTime <= t {
						continue
					}
				}
				dataBytes := kkv.ValueBytes()
				if len(dataBytes) < 2 {
					sequences = append(sequences, seq)
					continue
				}
				innerReader.Reset(dataBytes)
				// 读取版本号
				var protocalVersion, ifNullFlagVersion uint8
				binary.Read(innerReader, binary.LittleEndian, &protocalVersion)
				binary.Read(innerReader, binary.LittleEndian, &ifNullFlagVersion)
				readFeatureDBFunc_F_1 := func() (map[string]string, error) {
					properties := make(map[string]string)

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

						if _, exists := selectBehaviorFieldsSet[field]; exists {
							switch d.fieldTypeMap[field] {
							case constants.FS_INT32:
								var int32Value int32
								binary.Read(innerReader, binary.LittleEndian, &int32Value)
								properties[field] = fmt.Sprintf("%d", int32Value)
							case constants.FS_INT64:
								var int64Value int64
								binary.Read(innerReader, binary.LittleEndian, &int64Value)
								properties[field] = fmt.Sprintf("%d", int64Value)
							case constants.FS_FLOAT:
								var float32Value float32
								binary.Read(innerReader, binary.LittleEndian, &float32Value)
								properties[field] = fmt.Sprintf("%v", float32Value)
							case constants.FS_DOUBLE:
								var float64Value float64
								binary.Read(innerReader, binary.LittleEndian, &float64Value)
								properties[field] = fmt.Sprintf("%v", float64Value)
							case constants.FS_STRING:
								var length uint32
								binary.Read(innerReader, binary.LittleEndian, &length)
								strBytes := make([]byte, length)
								binary.Read(innerReader, binary.LittleEndian, &strBytes)
								properties[field] = string(strBytes)
							case constants.FS_BOOLEAN:
								var boolValue bool
								binary.Read(innerReader, binary.LittleEndian, &boolValue)
								properties[field] = fmt.Sprintf("%v", boolValue)
							default:
								var length uint32
								binary.Read(innerReader, binary.LittleEndian, &length)
								strBytes := make([]byte, length)
								binary.Read(innerReader, binary.LittleEndian, &strBytes)
								properties[field] = string(strBytes)
							}
						} else {
							skipBytes := CntSkipBytes(innerReader, d.fieldTypeMap[field])
							if skipBytes > 0 {
								if _, err := innerReader.Seek(int64(skipBytes), io.SeekCurrent); err != nil {
									return nil, err
								}
							}
						}
					}
					return properties, nil
				}

				if protocalVersion == FeatureDB_Protocal_Version_F && ifNullFlagVersion == FeatureDB_IfNull_Flag_Version_1 {
					readResult, err := readFeatureDBFunc_F_1()
					if err != nil {
						errChan <- err
						return nil
					}
					seq.onlineBehaviourTableFieldsMap = readResult
				} else {
					errChan <- fmt.Errorf("unsupported protocal version: %d, ifNullFlagVersion: %d", protocalVersion, ifNullFlagVersion)
					continue
				}
				sequences = append(sequences, seq)
			}
		}

		return sequences
	}

	results := make(map[string]interface{})

	var mu sync.Mutex

	var eventWg sync.WaitGroup
	for seqEvent, seqConfigs := range seqConfigsMap {
		if len(seqConfigs) == 0 {
			continue
		}
		seqConfigsBehaviorFields := seqConfigsBehaviorFieldsMap[seqEvent]
		maxLen := maxSeqLenMap[seqEvent]
		eventWg.Add(1)
		go func(seqEvent string, seqConfigs []*api.SeqConfig, maxLen int, seqConfigsBehaviorFields map[string]struct{}) {
			defer eventWg.Done()

			// FeatureDB has processed the integration of online sequence features and offline sequence features
			// Here we put the results into onlineSequences

			onlineSequences := fetchDataFunc(seqEvent, maxLen, keys, seqConfigsBehaviorFields)

			for _, seqConfig := range seqConfigs {
				var truncatedSequences []*sequenceInfo
				if seqConfig.SeqLen >= len(onlineSequences) {
					truncatedSequences = onlineSequences
				} else {
					truncatedSequences = onlineSequences[:seqConfig.SeqLen]
				}

				subproperties := makeSequenceFeatures4FeatureDB(truncatedSequences, seqConfig, sequenceConfig, currTime)
				mu.Lock()
				for k, value := range subproperties {
					results[k] = value
				}
				mu.Unlock()
			}
		}(seqEvent, seqConfigs, maxLen, seqConfigsBehaviorFields)
	}
	eventWg.Wait()

	if len(keys) > 0 {
		results[userIdField] = keys[0]
	}

	return results, nil
}

type FeatureDBScanKKVRequest struct {
	Prefixs   []string `json:"prefixs"`
	Length    int      `json:"length"`
	WithValue bool     `json:"with_value"`
}

func (d *FeatureViewFeatureDBDao) GetUserBehaviorFeature(userIds []interface{}, events []interface{}, selectFields []string, sequenceConfig api.FeatureViewSeqConfig) ([]map[string]interface{}, error) {
	selectFieldsSet := make(map[string]struct{})
	for _, selectField := range selectFields {
		selectFieldsSet[selectField] = struct{}{}
	}
	sequencePlayTimeMap := makePlayTimeMap(sequenceConfig.PlayTimeFilter)

	errChan := make(chan error, len(userIds))

	fetchDataFunc := func(user_id interface{}) []map[string]interface{} {
		results := []map[string]interface{}{}

		var response *http.Response
		if len(events) == 0 {
			prefixs := []string{fmt.Sprintf("%v\u001D", user_id)}
			request := FeatureDBScanKKVRequest{
				Prefixs:   prefixs,
				WithValue: true,
			}
			body, _ := json.Marshal(request)
			url := fmt.Sprintf("%s/api/v1/tables/%s/%s/%s/scan_kkv", d.featureDBClient.GetCurrentAddress(false), d.database, d.schema, d.table)
			req, err := http.NewRequest("POST", url, bytes.NewReader(body))
			if err != nil {
				errChan <- err
				return nil
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", d.featureDBClient.Token)
			req.Header.Set("Auth", d.signature)

			response, err = d.featureDBClient.Client.Do(req)
			if err != nil {
				url = fmt.Sprintf("%s/api/v1/tables/%s/%s/%s/scan_kkv", d.featureDBClient.GetCurrentAddress(true), d.database, d.schema, d.table)
				req, err = http.NewRequest("POST", url, bytes.NewReader(body))
				if err != nil {
					errChan <- err
					return nil
				}
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Authorization", d.featureDBClient.Token)
				req.Header.Set("Auth", d.signature)
				response, err = d.featureDBClient.Client.Do(req)
				if err != nil {
					errChan <- err
					return nil
				}
			}
		} else {
			pks := make([]string, 0, len(events))
			for _, event := range events {
				pks = append(pks, fmt.Sprintf("%v\u001D%v", user_id, event))
			}
			request := FeatureDBBatchGetKKVRequest{
				PKs:       pks,
				WithValue: true,
			}
			body, _ := json.Marshal(request)
			url := fmt.Sprintf("%s/api/v1/tables/%s/%s/%s/batch_get_kkv", d.featureDBClient.GetCurrentAddress(false), d.database, d.schema, d.table)
			req, err := http.NewRequest("POST", url, bytes.NewReader(body))
			if err != nil {
				errChan <- err
				return nil
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", d.featureDBClient.Token)
			req.Header.Set("Auth", d.signature)

			response, err = d.featureDBClient.Client.Do(req)
			if err != nil {
				url = fmt.Sprintf("%s/api/v1/tables/%s/%s/%s/batch_get_kkv", d.featureDBClient.GetCurrentAddress(true), d.database, d.schema, d.table)
				req, err = http.NewRequest("POST", url, bytes.NewReader(body))
				if err != nil {
					errChan <- err
					return nil
				}
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Authorization", d.featureDBClient.Token)
				req.Header.Set("Auth", d.signature)
				response, err = d.featureDBClient.Client.Do(req)
				if err != nil {
					errChan <- err
					return nil
				}
			}
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
		innerReader := readerPool.Get().(*bytes.Reader)
		defer readerPool.Put(innerReader)
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
				dataBytes := kkv.ValueBytes()
				if len(dataBytes) < 2 {
					//fmt.Println("userid ", user_id, " not exists")
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
							case constants.FS_INT32:
								var int32Value int32
								binary.Read(innerReader, binary.LittleEndian, &int32Value)
								properties[field] = int32Value
							case constants.FS_INT64:
								var int64Value int64
								binary.Read(innerReader, binary.LittleEndian, &int64Value)
								properties[field] = int64Value
							case constants.FS_FLOAT:
								var float32Value float32
								binary.Read(innerReader, binary.LittleEndian, &float32Value)
								properties[field] = float32Value
							case constants.FS_DOUBLE:
								var float64Value float64
								binary.Read(innerReader, binary.LittleEndian, &float64Value)
								properties[field] = float64Value
							case constants.FS_STRING:
								var length uint32
								binary.Read(innerReader, binary.LittleEndian, &length)
								strBytes := make([]byte, length)
								binary.Read(innerReader, binary.LittleEndian, &strBytes)
								properties[field] = string(strBytes)
							case constants.FS_BOOLEAN:
								var boolValue bool
								binary.Read(innerReader, binary.LittleEndian, &boolValue)
								properties[field] = boolValue
							default:
								var length uint32
								binary.Read(innerReader, binary.LittleEndian, &length)
								strBytes := make([]byte, length)
								binary.Read(innerReader, binary.LittleEndian, &strBytes)
								properties[field] = string(strBytes)
							}
						} else {
							skipBytes := CntSkipBytes(innerReader, d.fieldTypeMap[field])
							if skipBytes > 0 {
								if _, err := innerReader.Seek(int64(skipBytes), io.SeekCurrent); err != nil {
									return nil, err
								}
							}
						}
					}
					return properties, nil
				}

				if protocalVersion == FeatureDB_Protocal_Version_F && ifNullFlagVersion == FeatureDB_IfNull_Flag_Version_1 {
					readResult, err := readFeatureDBFunc_F_1()
					if err != nil {
						errChan <- err
						return nil
					}
					if t, exist := sequencePlayTimeMap[utils.ToString(readResult[sequenceConfig.EventField], "")]; exist {
						if utils.ToFloat(readResult[sequenceConfig.PlayTimeField], 0.0) <= t {
							continue
						}
					}
					results = append(results, readResult)
				} else {
					errChan <- fmt.Errorf("unsupported protocal version: %d, ifNullFlagVersion: %d", protocalVersion, ifNullFlagVersion)
					return nil
				}
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
func (d *FeatureViewFeatureDBDao) RowCountIds(filterExpr string) ([]string, int, error) {
	start := time.Now()
	snapshotId, _, err := d.createSnapshot()
	if err != nil {
		return nil, 0, err
	}
	var program *vm.Program
	var fieldNames []string
	if filterExpr != "" {
		program, err = expr.Compile(filterExpr)
		if err != nil {
			return nil, 0, err
		}
		if names, err := ExtractVariables(filterExpr); err != nil {
			return nil, 0, err
		} else {
			fieldNames = names
		}
	}

	alloc := memory.NewGoAllocator()
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/api/v1/tables/%s/%s/%s/snapshots/%s/scan",
		d.featureDBClient.GetCurrentAddress(false), d.database, d.schema, d.table, snapshotId), bytes.NewReader(nil))
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", d.featureDBClient.Token)
	req.Header.Set("Auth", d.signature)
	response, err := d.featureDBClient.Client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		return nil, 0, fmt.Errorf("status code: %d, response body: %s", response.StatusCode, string(body))
	}

	// Arrow IPC reader
	reader, _ := ipc.NewReader(response.Body, ipc.WithAllocator(alloc))

	readFeatureDBFunc_F_1 := func(cursor *utils.ByteCursor, fieldNames []string) (map[string]interface{}, error) {
		properties := make(map[string]interface{})
		if len(fieldNames) == 0 {
			return properties, nil
		}

		selectFieldsSet := make(map[string]struct{}, len(fieldNames))
		for _, field := range fieldNames {
			selectFieldsSet[field] = struct{}{}
		}
		for _, field := range d.fields {
			isNull, ok := cursor.TryReadUint8()
			if !ok {
				// EOF
				break
			}
			if isNull == 1 {
				// 跳过空值
				continue
			}
			if len(properties) == len(fieldNames) {
				break
			}

			if _, exists := selectFieldsSet[field]; exists {
				switch d.fieldTypeMap[field] {
				case constants.FS_DOUBLE:
					properties[field] = cursor.ReadFloat64()
				case constants.FS_FLOAT:
					properties[field] = cursor.ReadFloat32()
				case constants.FS_INT64:
					properties[field] = cursor.ReadInt64()
				case constants.FS_INT32:
					properties[field] = cursor.ReadInt32()
				case constants.FS_BOOLEAN:
					properties[field] = cursor.ReadBool()
				case constants.FS_STRING:
					properties[field] = cursor.ReadString()
				case constants.FS_TIMESTAMP:
					properties[field] = time.UnixMilli(cursor.ReadInt64())
				case constants.FS_ARRAY_INT32:
					properties[field] = cursor.ReadInt32Slice(cursor.ReadUint32())
				case constants.FS_ARRAY_INT64:
					properties[field] = cursor.ReadInt64Slice(cursor.ReadUint32())
				case constants.FS_ARRAY_FLOAT:
					properties[field] = cursor.ReadFloat32Slice(cursor.ReadUint32())
				case constants.FS_ARRAY_DOUBLE:
					properties[field] = cursor.ReadFloat64Slice(cursor.ReadUint32())
				case constants.FS_ARRAY_STRING:
					properties[field] = cursor.ReadStringArray(cursor.ReadUint32())
				case constants.FS_ARRAY_ARRAY_FLOAT:
					outerLength := cursor.ReadUint32()
					if outerLength == 0 {
						properties[field] = [][]float32{}
					} else {
						totalElements := cursor.ReadUint32()
						if totalElements == 0 {
							arr := make([][]float32, outerLength)
							for k := uint32(0); k < outerLength; k++ {
								arr[k] = []float32{}
							}
							properties[field] = arr
						} else {
							innerArrayLens := cursor.ReadUint32Slice(outerLength)
							innerValidElements := cursor.ReadFloat32Slice(totalElements)
							result := make([][]float32, outerLength)
							innerIndex := 0
							for outerIdx, innerLength := range innerArrayLens {
								result[outerIdx] = innerValidElements[innerIndex : innerIndex+int(innerLength)]
								innerIndex += int(innerLength)
							}
							properties[field] = result
						}
					}
				case constants.FS_MAP_INT32_INT32:
					length := cursor.ReadUint32()
					mapInt32Int32Value := make(map[int32]int32, length)
					if length > 0 {
						keys := cursor.ReadInt32Slice(length)
						values := cursor.ReadInt32Slice(length)
						for k := uint32(0); k < length; k++ {
							mapInt32Int32Value[keys[k]] = values[k]
						}
					}
					properties[field] = mapInt32Int32Value
				case constants.FS_MAP_INT32_INT64:
					length := cursor.ReadUint32()
					mapInt32Int64Value := make(map[int32]int64, length)
					if length > 0 {
						keys := cursor.ReadInt32Slice(length)
						values := cursor.ReadInt64Slice(length)
						for k := uint32(0); k < length; k++ {
							mapInt32Int64Value[keys[k]] = values[k]
						}
					}
					properties[field] = mapInt32Int64Value
				case constants.FS_MAP_INT32_FLOAT:
					length := cursor.ReadUint32()
					mapInt32FloatValue := make(map[int32]float32, length)
					if length > 0 {
						keys := cursor.ReadInt32Slice(length)
						values := cursor.ReadFloat32Slice(length)
						for k := uint32(0); k < length; k++ {
							mapInt32FloatValue[keys[k]] = values[k]
						}
					}
					properties[field] = mapInt32FloatValue
				case constants.FS_MAP_INT32_DOUBLE:
					length := cursor.ReadUint32()
					mapInt32DoubleValue := make(map[int32]float64, length)
					if length > 0 {
						keys := cursor.ReadInt32Slice(length)
						values := cursor.ReadFloat64Slice(length)
						for k := uint32(0); k < length; k++ {
							mapInt32DoubleValue[keys[k]] = values[k]
						}
					}
					properties[field] = mapInt32DoubleValue
				case constants.FS_MAP_INT32_STRING:
					length := cursor.ReadUint32()
					mapInt32StringValue := make(map[int32]string, length)
					if length > 0 {
						keys := cursor.ReadInt32Slice(length)
						values := cursor.ReadStringArray(length)
						for k := uint32(0); k < length; k++ {
							mapInt32StringValue[keys[k]] = values[k]
						}
					}
					properties[field] = mapInt32StringValue
				case constants.FS_MAP_INT64_INT32:
					length := cursor.ReadUint32()
					mapInt64Int32Value := make(map[int64]int32, length)
					if length > 0 {
						keys := cursor.ReadInt64Slice(length)
						values := cursor.ReadInt32Slice(length)
						for k := uint32(0); k < length; k++ {
							mapInt64Int32Value[keys[k]] = values[k]
						}
					}
					properties[field] = mapInt64Int32Value
				case constants.FS_MAP_INT64_INT64:
					length := cursor.ReadUint32()
					mapInt64Int64Value := make(map[int64]int64, length)
					if length > 0 {
						keys := cursor.ReadInt64Slice(length)
						values := cursor.ReadInt64Slice(length)
						for k := uint32(0); k < length; k++ {
							mapInt64Int64Value[keys[k]] = values[k]
						}
					}
					properties[field] = mapInt64Int64Value
				case constants.FS_MAP_INT64_FLOAT:
					length := cursor.ReadUint32()
					mapInt64FloatValue := make(map[int64]float32, length)
					if length > 0 {
						keys := cursor.ReadInt64Slice(length)
						values := cursor.ReadFloat32Slice(length)
						for k := uint32(0); k < length; k++ {
							mapInt64FloatValue[keys[k]] = values[k]
						}
					}
					properties[field] = mapInt64FloatValue
				case constants.FS_MAP_INT64_DOUBLE:
					length := cursor.ReadUint32()
					mapInt64DoubleValue := make(map[int64]float64, length)
					if length > 0 {
						keys := cursor.ReadInt64Slice(length)
						values := cursor.ReadFloat64Slice(length)
						for k := uint32(0); k < length; k++ {
							mapInt64DoubleValue[keys[k]] = values[k]
						}
					}
					properties[field] = mapInt64DoubleValue
				case constants.FS_MAP_INT64_STRING:
					length := cursor.ReadUint32()
					mapInt64StringValue := make(map[int64]string, length)
					if length > 0 {
						keys := cursor.ReadInt64Slice(length)
						values := cursor.ReadStringArray(length)
						for k := uint32(0); k < length; k++ {
							mapInt64StringValue[keys[k]] = values[k]
						}
					}
					properties[field] = mapInt64StringValue
				case constants.FS_MAP_STRING_INT32:
					length := cursor.ReadUint32()
					mapStringInt32Value := make(map[string]int32, length)
					if length > 0 {
						keys := cursor.ReadStringArray(length)
						values := cursor.ReadInt32Slice(length)
						for k := uint32(0); k < length; k++ {
							mapStringInt32Value[keys[k]] = values[k]
						}
					}
					properties[field] = mapStringInt32Value
				case constants.FS_MAP_STRING_INT64:
					length := cursor.ReadUint32()
					mapStringInt64Value := make(map[string]int64, length)
					if length > 0 {
						keys := cursor.ReadStringArray(length)
						values := cursor.ReadInt64Slice(length)
						for k := uint32(0); k < length; k++ {
							mapStringInt64Value[keys[k]] = values[k]
						}
					}
					properties[field] = mapStringInt64Value
				case constants.FS_MAP_STRING_FLOAT:
					length := cursor.ReadUint32()
					mapStringFloatValue := make(map[string]float32, length)
					if length > 0 {
						keys := cursor.ReadStringArray(length)
						values := cursor.ReadFloat32Slice(length)
						for k := uint32(0); k < length; k++ {
							mapStringFloatValue[keys[k]] = values[k]
						}
					}
					properties[field] = mapStringFloatValue
				case constants.FS_MAP_STRING_DOUBLE:
					length := cursor.ReadUint32()
					mapStringDoubleValue := make(map[string]float64, length)
					if length > 0 {
						keys := cursor.ReadStringArray(length)
						values := cursor.ReadFloat64Slice(length)
						for k := uint32(0); k < length; k++ {
							mapStringDoubleValue[keys[k]] = values[k]
						}
					}
					properties[field] = mapStringDoubleValue
				case constants.FS_MAP_STRING_STRING:
					length := cursor.ReadUint32()
					mapStringStringValue := make(map[string]string, length)
					if length > 0 {
						keys := cursor.ReadStringArray(length)
						values := cursor.ReadStringArray(length)
						for k := uint32(0); k < length; k++ {
							mapStringStringValue[keys[k]] = values[k]
						}
					}
					properties[field] = mapStringStringValue
				default:
					properties[field] = cursor.ReadString()
				}
			} else {
				switch d.fieldTypeMap[field] {
				case constants.FS_DOUBLE:
					cursor.Skip(8)
				case constants.FS_FLOAT:
					cursor.Skip(4)
				case constants.FS_INT64:
					cursor.Skip(8)
				case constants.FS_INT32:
					cursor.Skip(4)
				case constants.FS_BOOLEAN:
					cursor.Skip(1)
				case constants.FS_STRING:
					cursor.Skip(int(cursor.ReadUint32()))
				case constants.FS_TIMESTAMP:
					cursor.Skip(8)
				case constants.FS_ARRAY_INT32:
					cursor.Skip(int(cursor.ReadUint32()) * 4)
				case constants.FS_ARRAY_INT64:
					cursor.Skip(int(cursor.ReadUint32()) * 8)
				case constants.FS_ARRAY_FLOAT:
					cursor.Skip(int(cursor.ReadUint32()) * 4)
				case constants.FS_ARRAY_DOUBLE:
					cursor.Skip(int(cursor.ReadUint32()) * 8)
				case constants.FS_ARRAY_STRING:
					cursor.SkipStringArray(cursor.ReadUint32())
				case constants.FS_ARRAY_ARRAY_FLOAT:
					outerLength := cursor.ReadUint32()
					if outerLength > 0 {
						totalElements := cursor.ReadUint32()
						if totalElements > 0 {
							cursor.Skip(int(outerLength*4 + totalElements*4))
						}
					}
				case constants.FS_MAP_INT32_INT32:
					cursor.Skip(int(cursor.ReadUint32()) * (4 + 4))
				case constants.FS_MAP_INT32_INT64:
					cursor.Skip(int(cursor.ReadUint32()) * (4 + 8))
				case constants.FS_MAP_INT32_FLOAT:
					cursor.Skip(int(cursor.ReadUint32()) * (4 + 4))
				case constants.FS_MAP_INT32_DOUBLE:
					cursor.Skip(int(cursor.ReadUint32()) * (4 + 8))
				case constants.FS_MAP_INT32_STRING:
					length := cursor.ReadUint32()
					cursor.Skip(int(length * 4))
					cursor.SkipStringArray(length)
				case constants.FS_MAP_INT64_INT32:
					cursor.Skip(int(cursor.ReadUint32()) * (8 + 4))
				case constants.FS_MAP_INT64_INT64:
					cursor.Skip(int(cursor.ReadUint32()) * (8 + 8))
				case constants.FS_MAP_INT64_FLOAT:
					cursor.Skip(int(cursor.ReadUint32()) * (8 + 4))
				case constants.FS_MAP_INT64_DOUBLE:
					cursor.Skip(int(cursor.ReadUint32()) * (8 + 8))
				case constants.FS_MAP_INT64_STRING:
					length := cursor.ReadUint32()
					cursor.Skip(int(length * 8))
					cursor.SkipStringArray(length)
				case constants.FS_MAP_STRING_INT32:
					length := cursor.ReadUint32()
					cursor.SkipStringArray(length)
					cursor.Skip(int(length * 4))
				case constants.FS_MAP_STRING_INT64:
					length := cursor.ReadUint32()
					cursor.SkipStringArray(length)
					cursor.Skip(int(length * 8))
				case constants.FS_MAP_STRING_FLOAT:
					length := cursor.ReadUint32()
					cursor.SkipStringArray(length)
					cursor.Skip(int(length * 4))
				case constants.FS_MAP_STRING_DOUBLE:
					length := cursor.ReadUint32()
					cursor.SkipStringArray(length)
					cursor.Skip(int(length * 8))
				case constants.FS_MAP_STRING_STRING:
					length := cursor.ReadUint32()
					cursor.SkipStringArray(length)
					cursor.SkipStringArray(length)
				default:
					cursor.Skip(int(cursor.ReadUint32()))
				}
			}
			if cursor.Err != nil {
				return nil, cursor.Err
			}
		}
		return properties, nil
	}
	ids := make([]string, 0, 1024)
	for reader.Next() {
		record := reader.Record()
		for i := 0; i < int(record.NumRows()); i++ {
			if filterExpr == "" {
				ids = append(ids, record.Column(0).(*array.String).Value(i))
			} else {
				dataBytes := record.Column(1).(*array.Binary).Value(i)
				if len(dataBytes) < 2 {
					continue
				}
				cursor := utils.NewByteCursor(dataBytes)

				// 读取版本号
				protocalVersion := cursor.ReadUint8()
				ifNullFlagVersion := cursor.ReadUint8()
				if protocalVersion != FeatureDB_Protocal_Version_F || ifNullFlagVersion != FeatureDB_IfNull_Flag_Version_1 {
					return nil, 0, errors.New("protocal version or if null flag version is not supported")
				}
				properties, err := readFeatureDBFunc_F_1(cursor, fieldNames)
				if err != nil {
					return nil, 0, err
				}
				if ret, err := expr.Run(program, properties); err != nil {
					return nil, 0, err
				} else if r, ok := ret.(bool); ok && r {
					ids = append(ids, record.Column(0).(*array.String).Value(i))
				}
			}
		}
		record.Release()
	}
	log.Printf("RowCountIds size:%d, cost:%d\n", len(ids), time.Since(start)/time.Millisecond)
	return ids, len(ids), nil
}

func (d *FeatureViewFeatureDBDao) createSnapshot() (string, int64, error) {
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/api/v1/tables/%s/%s/%s/snapshots",
		d.featureDBClient.GetNormalAddress(), d.database, d.schema, d.table), bytes.NewReader(nil))
	if err != nil {
		return "", 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", d.featureDBClient.Token)
	req.Header.Set("Auth", d.signature)
	response, err := d.featureDBClient.Client.Do(req)
	if err != nil {
		return "", 0, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		return "", 0, fmt.Errorf("status code: %d, response body: %s", response.StatusCode, string(body))
	}
	resonseBody := struct {
		RequestId string         `json:"request_id,omitempty"`
		Code      string         `json:"code"`
		Message   string         `json:"message,omitempty"`
		Data      map[string]any `json:"data,omitempty"`
	}{}

	decoder := json.NewDecoder(response.Body)
	decoder.UseNumber()
	if err := decoder.Decode(&resonseBody); err != nil {
		return "", 0, err
	}

	return resonseBody.Data["snapshot_id"].(string), utils.ToInt64(resonseBody.Data["ts"], 0), nil
}

func (d *FeatureViewFeatureDBDao) ScanAndIterateData(filter string, ch chan<- string) ([]string, error) {
	_, ts, err := d.createSnapshot()
	if err != nil {
		return nil, err
	}
	var program *vm.Program
	if filter != "" {
		program, err = expr.Compile(filter)
		if err != nil {
			return nil, err
		}
	}

	ids, _, err := d.RowCountIds(filter)

	if err != nil {
		return nil, err
	}
	readFeatureDBFunc_F_1 := func(innerReader *bytes.Reader) (map[string]interface{}, error) {
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

			switch d.fieldTypeMap[field] {
			case constants.FS_INT32:
				var int32Value int32
				binary.Read(innerReader, binary.LittleEndian, &int32Value)
				properties[field] = int32Value
			case constants.FS_INT64:
				var int64Value int64
				binary.Read(innerReader, binary.LittleEndian, &int64Value)
				properties[field] = int64Value
			case constants.FS_FLOAT:
				var float32Value float32
				binary.Read(innerReader, binary.LittleEndian, &float32Value)
				properties[field] = float32Value
			case constants.FS_DOUBLE:
				var float64Value float64
				binary.Read(innerReader, binary.LittleEndian, &float64Value)
				properties[field] = float64Value
			case constants.FS_STRING:
				var length uint32
				binary.Read(innerReader, binary.LittleEndian, &length)
				strBytes := make([]byte, length)
				binary.Read(innerReader, binary.LittleEndian, &strBytes)
				properties[field] = string(strBytes)
			case constants.FS_BOOLEAN:
				var boolValue bool
				binary.Read(innerReader, binary.LittleEndian, &boolValue)
				properties[field] = boolValue
			default:
				var length uint32
				binary.Read(innerReader, binary.LittleEndian, &length)
				strBytes := make([]byte, length)
				binary.Read(innerReader, binary.LittleEndian, &strBytes)
				properties[field] = string(strBytes)
			}
		}
		return properties, nil
	}
	if ch != nil {
		go func() {
			alloc := memory.NewGoAllocator()
			for {
				time.Sleep(time.Second * 5)
				req, err := http.NewRequest("GET", fmt.Sprintf("%s/api/v1/tables/%s/%s/%s/iterate_get_kv?ts=%d",
					d.featureDBClient.GetNormalAddress(), d.database, d.schema, d.table, ts), bytes.NewReader(nil))
				if err != nil {
					continue
				}
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Authorization", d.featureDBClient.Token)
				req.Header.Set("Auth", d.signature)
				response, err := d.featureDBClient.Client.Do(req)
				if err != nil {
					continue
				}
				if response.StatusCode != http.StatusOK {
					continue
				}
				_ts := utils.ToInt64(response.Header.Get("Next-Ts"), 0)
				if _ts == 0 {
					continue
				}
				ts = _ts
				reader, _ := ipc.NewReader(response.Body, ipc.WithAllocator(alloc))

				innerReader := readerPool.Get().(*bytes.Reader)
				for reader.Next() {
					record := reader.Record()
					for i := 0; i < int(record.NumRows()); i++ {
						if filter == "" {
							ch <- record.Column(0).(*array.String).Value(i)
						} else {
							dataBytes := record.Column(1).(*array.Binary).Value(i)
							if len(dataBytes) < 2 {
								continue
							}
							innerReader.Reset(dataBytes)

							// 读取版本号
							var protocalVersion, ifNullFlagVersion uint8
							binary.Read(innerReader, binary.LittleEndian, &protocalVersion)
							binary.Read(innerReader, binary.LittleEndian, &ifNullFlagVersion)
							properties, err := readFeatureDBFunc_F_1(innerReader)
							if err != nil {
								continue
							}
							if ret, err := expr.Run(program, properties); err != nil {
								continue
							} else if r, ok := ret.(bool); ok && r {
								ch <- record.Column(0).(*array.String).Value(i)
							}
						}
					}

					record.Release()
				}
				readerPool.Put(innerReader)
				response.Body.Close()

			}

		}()
	}

	return ids, nil
}
