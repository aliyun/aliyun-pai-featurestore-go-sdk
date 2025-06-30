package common

import (
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"
)

const timeFormat = "2006-01-02 15:04:05.000 -0700 MST"

func formatTimeIfPossible(v interface{}) (string, bool) {
	if t, ok := v.(time.Time); ok {
		return t.Format(timeFormat), true
	}
	if s, ok := v.(string); ok {
		return s, true
	}
	return "", false
}

func deepEqual(a, b interface{}) bool {
	if aStr, aOk := formatTimeIfPossible(a); aOk {
		if bStr, bOk := formatTimeIfPossible(b); bOk {
			return aStr == bStr
		}
	}

	strA := fmt.Sprintf("%v", a)
	strB := fmt.Sprintf("%v", b)

	if strA == strB {
		return true
	}

	if reflect.TypeOf(a).Kind() != reflect.TypeOf(b).Kind() {
		return false
	}

	va := reflect.ValueOf(a)
	vb := reflect.ValueOf(b)

	switch va.Kind() {
	case reflect.Array, reflect.Slice:
		if va.Len() != vb.Len() {
			return false
		}
		for i := 0; i < va.Len(); i++ {
			if !deepEqual(va.Index(i).Interface(), vb.Index(i).Interface()) {
				return false
			}
		}
		return true
	case reflect.Map:
		if va.Len() != vb.Len() {
			return false
		}
		for _, k := range va.MapKeys() {
			if !deepEqual(va.MapIndex(k).Interface(), vb.MapIndex(k).Interface()) {
				return false
			}
		}
		return true
	}

	return strA == strB
}

func CheckResults(joinIdName string, results []map[string]interface{}, expectedResults []map[string]interface{}, isSequenceFeatureViewData bool) (bool, error) {
	if len(results) != len(expectedResults) {
		return false, errors.New(fmt.Sprintf("results length(%d) not equal to expected results length(%d)", len(results), len(expectedResults)))
	}
	expectedResultsIdxMap := make(map[string]int)
	for i, expectedResult := range expectedResults {
		if joinId, ok := expectedResult[joinIdName]; ok {
			expectedResultsIdxMap[fmt.Sprintf("%v", joinId)] = i
		} else {
			return false, errors.New("join id value not found in expected result")
		}
	}

	for i, result := range results {
		if joinId, ok := result[joinIdName]; ok {
			if expectedResultIdx, ok := expectedResultsIdxMap[fmt.Sprintf("%v", joinId)]; ok {
				expectedResult := expectedResults[expectedResultIdx]
				for key, value := range result {
					if isSequenceFeatureViewData && strings.HasSuffix(key, "_ts") {
						continue
					}
					expectedVal, expectedOK := expectedResult[key]
					if expectedOK {
						if !deepEqual(value, expectedVal) {
							return false, fmt.Errorf("feature field %v: result value:%vnot equal to expected value:%v (the current row %s == join_id is %v, line is %d)", key, value, expectedResult[key], joinIdName, result[joinIdName], i)
						}
					} else {
						return false, fmt.Errorf("feature field %v: result value:%v expected value is nil (the current row %s == join_id is %v, line is %d)", key, value, joinIdName, result[joinIdName], i)
					}
				}
			} else {
				return false, fmt.Errorf("join id %v not found in expected results", joinId)
			}
		} else {
			return false, errors.New("join id value not found in result")
		}
	}

	return true, nil
}

func CheckResultsWithFile(joinIdName string, results []map[string]interface{}, filePath string, isSequenceFeatureViewData bool) (bool, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return false, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()

	if err != nil {
		return false, fmt.Errorf("failed to read CSV file: %w", err)
	}
	if len(records) < 2 {
		return false, fmt.Errorf("CSV file does not contain any data")
	}

	headers := records[0]
	var expectedResults []map[string]interface{}

	for _, record := range records[1:] {
		result := make(map[string]interface{})
		for i, value := range record {
			if value == `\N` {
				continue
			}
			result[headers[i]] = value
		}
		expectedResults = append(expectedResults, result)
	}

	return CheckResults(joinIdName, results, expectedResults, isSequenceFeatureViewData)
}

func CheckBehaviorResults(userIdField, eventField, itemIdField, eventTimeField string, results []map[string]interface{}, expectedResults []map[string]interface{}) (bool, error) {
	if len(results) != len(expectedResults) {
		return false, errors.New(fmt.Sprintf("results length(%d) not equal to expected results length(%d)", len(results), len(expectedResults)))
	}
	expectedResultsIdxMap := make(map[string]int)
	for i, expectedResult := range expectedResults {
		userID, userOK := expectedResult[userIdField]
		itemID, itemOK := expectedResult[itemIdField]
		event, eventOK := expectedResult[eventField]
		eventTime, timeOK := expectedResult[eventTimeField]

		if !userOK || !itemOK || !eventOK || !timeOK {
			return false, errors.New("one or more key fields are missing in expected result")
		}

		// 构造唯一键
		key := fmt.Sprintf("%v_%v_%v_%v", userID, itemID, event, eventTime)
		if existingIndex, exists := expectedResultsIdxMap[key]; exists {
			return false, fmt.Errorf("duplicate key detected: %s at index %d and %d", key, existingIndex, i)
		} else {
			expectedResultsIdxMap[key] = i
		}

	}

	for i, result := range results {
		userID, userOK := result[userIdField]
		itemID, itemOK := result[itemIdField]
		event, eventOK := result[eventField]
		eventTime, timeOK := result[eventTimeField]

		if !userOK || !itemOK || !eventOK || !timeOK {
			return false, errors.New("one or more key fields are missing in result")
		}
		// 构造唯一键
		onlyId := fmt.Sprintf("%v_%v_%v_%v", userID, itemID, event, eventTime)
		if expectedResultIdx, ok := expectedResultsIdxMap[fmt.Sprintf("%s", onlyId)]; ok {
			expectedResult := expectedResults[expectedResultIdx]
			for key, value := range result {
				expectedval, expectedOK := expectedResult[key]
				if expectedOK {
					if !deepEqual(value, expectedval) {
						return false, fmt.Errorf("feature field %v: result value:%vnot equal to expected value:%v(the current row %s == join_id is %v, line is %d)", key, value, expectedResult[key], key, result[onlyId], i)
					}
				}
			}
		} else {
			return false, fmt.Errorf("join id %v not found in expected results", onlyId)
		}

	}

	return true, nil
}

func CheckBehaviorResultsWithFile(userIdField, eventField, itemIdField, eventTimeField string, results []map[string]interface{}, filePath string) (bool, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return false, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()

	if err != nil {
		return false, fmt.Errorf("failed to read CSV file: %w", err)
	}
	if len(records) < 2 {
		return false, fmt.Errorf("CSV file does not contain any data")
	}

	headers := records[0]
	var expectedResults []map[string]interface{}

	for _, record := range records[1:] {
		result := make(map[string]interface{})
		for i, value := range record {
			if value == `\N` {
				continue
			}
			result[headers[i]] = value
		}
		expectedResults = append(expectedResults, result)
	}

	return CheckBehaviorResults(userIdField, eventField, itemIdField, eventTimeField, results, expectedResults)
}
