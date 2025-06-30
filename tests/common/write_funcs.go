package common

import (
	"bytes"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"time"

	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/constants"
	constants2 "github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/tests/constants"
	"github.com/google/uuid"
)

var (
	featureServiceAddress = os.Getenv("FEATURE_SERVICE_ADDRESS")

	featureServiceToken = os.Getenv("FEATURE_SERVICE_TOKEN")
)

var httpClient *http.Client

func init() {
	httpClient = &http.Client{
		Transport: &http.Transport{
			MaxConnsPerHost: 1000,
			MaxIdleConns:    1000,
		},
	}
}

func signature(username, password string) (string, error) {
	auth := username + ":" + password
	return base64.StdEncoding.EncodeToString([]byte(auth)), nil
}

type ResponseBody struct {
	RequestId string                 `json:"requestId,omitempty"`
	Code      string                 `json:"code"`
	Message   string                 `json:"message,omitempty"`
	Data      map[string]interface{} `json:"data,omitempty"`
}

func WriteKVFeatures(region, instanceId, projectName, featureViewName, featureDBUserName, featureDBPassword string, datas []map[string]any, writeMode int) error {
	data := map[string]any{
		"content": datas,
	}
	if writeMode == constants2.PartialFieldWrite {
		data["write_mode"] = "PartialFieldWrite"
	}

	dataStr, _ := json.Marshal(data)
	req, _ := http.NewRequest("POST", fmt.Sprintf("%s/api/v1/tables/%s/%s/%s/write", featureServiceAddress, instanceId, projectName, featureViewName), bytes.NewReader(dataStr))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", featureServiceToken)
	digest, _ := signature(featureDBUserName, featureDBPassword)
	req.Header.Set("Auth", digest)
	response, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	body, _ := io.ReadAll(response.Body)
	response.Body.Close()

	resp := &ResponseBody{}
	json.Unmarshal(body, resp)
	if resp.Code != "OK" {
		return fmt.Errorf("write kv features failed, code: %s, message: %s", resp.Code, resp.Message)
	}

	return nil
}

func writeCSV(filePath, joinIdName string, featureNames []string, featureNameMap map[string]constants.FSType, data []map[string]interface{}) error {
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// write header
	header := append([]string{joinIdName}, featureNames...)
	if err := writer.Write(header); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	// write data
	for _, row := range data {
		var record []string
		for _, field := range header {
			if value, exists := row[field]; exists {
				switch featureNameMap[field] {
				case constants.FS_TIMESTAMP:
					millis := value.(int64)
					t := time.UnixMilli(millis)
					record = append(record, t.Format("2006-01-02 15:04:05.000 -0700 MST"))
				default:
					record = append(record, fmt.Sprintf("%v", value))
				}
			} else {
				record = append(record, `\N`)
			}
		}
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write data: %w", err)
		}
	}

	return nil
}

func WriteKVFeaturesRandomly(region, instanceId, projectName, featureViewName, featureDBUserName, featureDBPassword, joinIdName string, featureNameMap map[string]constants.FSType, joinIds []interface{}, writeMode int) (string, error) {
	rand.Seed(time.Now().UnixNano())
	var data []map[string]interface{}
	featureNames := make([]string, 0, len(featureNameMap))

	for _, joinId := range joinIds {
		row := make(map[string]interface{})
		row[joinIdName] = joinId
		for featureName, featureType := range featureNameMap {
			featureNames = append(featureNames, featureName)

			switch featureType {
			case constants.FS_INT32, constants.FS_INT64:
				row[featureName] = rand.Intn(100)
			case constants.FS_FLOAT:
				row[featureName] = 100 * rand.Float32()
			case constants.FS_DOUBLE:
				row[featureName] = 100 * rand.Float64()
			case constants.FS_STRING:
				row[featureName] = fmt.Sprintf("test_%d", rand.Intn(100))
			case constants.FS_BOOLEAN:
				row[featureName] = rand.Intn(2) == 1
			case constants.FS_TIMESTAMP:
				row[featureName] = time.Now().UnixMilli() //毫秒
			case constants.FS_ARRAY_INT32, constants.FS_ARRAY_INT64:
				row[featureName] = []int{rand.Intn(100), rand.Intn(100)}
			case constants.FS_ARRAY_FLOAT:
				row[featureName] = []float32{100 * rand.Float32(), 100 * rand.Float32()}
			case constants.FS_ARRAY_DOUBLE:
				row[featureName] = []float64{100 * rand.Float64(), 100 * rand.Float64()}
			case constants.FS_ARRAY_STRING:
				row[featureName] = []string{fmt.Sprintf("test_%d", rand.Intn(100)), fmt.Sprintf("test_%d", rand.Intn(100))}
			case constants.FS_ARRAY_ARRAY_FLOAT:
				row[featureName] = [][]float32{{100 * rand.Float32(), 100 * rand.Float32()}, {100 * rand.Float32(), 100 * rand.Float32()}}
			case constants.FS_MAP_INT32_INT32, constants.FS_MAP_INT32_INT64, constants.FS_MAP_INT64_INT32, constants.FS_MAP_INT64_INT64:
				row[featureName] = map[int]int{rand.Intn(100): rand.Intn(100), rand.Intn(100): rand.Intn(100)}
			case constants.FS_MAP_INT32_FLOAT, constants.FS_MAP_INT64_FLOAT:
				row[featureName] = map[int]float32{rand.Intn(100): 100 * rand.Float32(), rand.Intn(100): 100 * rand.Float32()}
			case constants.FS_MAP_INT32_DOUBLE, constants.FS_MAP_INT64_DOUBLE:
				row[featureName] = map[int]float64{rand.Intn(100): 100 * rand.Float64(), rand.Intn(100): 100 * rand.Float64()}
			case constants.FS_MAP_INT32_STRING, constants.FS_MAP_INT64_STRING:
				row[featureName] = map[int]string{rand.Intn(100): fmt.Sprintf("test_%d", rand.Intn(100)), rand.Intn(100): fmt.Sprintf("test_%d", rand.Intn(100))}
			case constants.FS_MAP_STRING_INT32, constants.FS_MAP_STRING_INT64:
				row[featureName] = map[string]int{fmt.Sprintf("test_%d", rand.Intn(100)): rand.Intn(100), fmt.Sprintf("test_%d", rand.Intn(100)): rand.Intn(100)}
			case constants.FS_MAP_STRING_FLOAT:
				row[featureName] = map[string]float32{fmt.Sprintf("test_%d", rand.Intn(100)): 100 * rand.Float32(), fmt.Sprintf("test_%d", rand.Intn(100)): 100 * rand.Float32()}
			case constants.FS_MAP_STRING_DOUBLE:
				row[featureName] = map[string]float64{fmt.Sprintf("test_%d", rand.Intn(100)): 100 * rand.Float64(), fmt.Sprintf("test_%d", rand.Intn(100)): 100 * rand.Float64()}
			case constants.FS_MAP_STRING_STRING:
				row[featureName] = map[string]string{fmt.Sprintf("test_%d", rand.Intn(100)): fmt.Sprintf("test_%d", rand.Intn(100)), fmt.Sprintf("test_%d", rand.Intn(100)): fmt.Sprintf("test_%d", rand.Intn(100))}
			default:
				return "", fmt.Errorf("unsupported feature type: %v", featureType)
			}
		}
		data = append(data, row)
	}

	timestamp := time.Now().Format("20060102150405")
	fileName := fmt.Sprintf("%s_%s.csv", featureViewName, timestamp)
	filePath := fmt.Sprintf("../test_cases/expected_results/%s", fileName)
	err := writeCSV(filePath, joinIdName, featureNames, featureNameMap, data)
	if err != nil {
		return "", err
	}

	err = WriteKVFeatures(region, instanceId, projectName, featureViewName, featureDBUserName, featureDBPassword, data, writeMode)
	if err != nil {
		return "", err
	}

	return fileName, nil
}

func WriteKVFeaturesExpireWithEventTime(region, instanceId, projectName, featureViewName, featureDBUserName, featureDBPassword, joinIdName string, featureNameMap map[string]constants.FSType, joinIds []interface{}, featureViewLifeCycle int64, writeMode int) (string, error) {
	data := make([]map[string]interface{}, 0)
	featureNames := make([]string, 0, len(featureNameMap))
	ts := time.Now().AddDate(0, 0, -int(featureViewLifeCycle)).Add(10 * time.Second).UnixMilli()
	for _, i := range joinIds {
		datamap := make(map[string]interface{}, 4)
		itemString := uuid.New().String()[:6]
		datamap[joinIdName] = i
		for featureName, featureType := range featureNameMap {
			featureNames = append(featureNames, featureName)
			switch featureType {
			case constants.FS_INT32:
				datamap[featureName] = rand.Intn(110)
			case constants.FS_INT64:
				datamap[featureName] = ts
			case constants.FS_STRING:
				datamap[featureName] = itemString
			default:
				return "", fmt.Errorf("unsupported feature type: %v", featureType)
			}
		}
		data = append(data, datamap)
	}

	timestamp := time.Now().Format("20060102150405")
	fileName := fmt.Sprintf("%s_%s.csv", featureViewName, timestamp)
	filePath := fmt.Sprintf("../test_cases/expected_results/%s", fileName)
	err := writeCSV(filePath, joinIdName, featureNames, featureNameMap, data)
	if err != nil {
		return "", err
	}

	err = WriteKVFeatures(region, instanceId, projectName, featureViewName, featureDBUserName, featureDBPassword, data, writeMode)
	if err != nil {
		return "", err
	}

	return fileName, nil
}
