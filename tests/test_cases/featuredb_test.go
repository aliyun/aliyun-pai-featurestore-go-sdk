package testcases

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/constants"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/featurestore"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/tests/common"
	constants2 "github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/tests/constants"
)

var (
	regionId              = "cn-beijing"
	instanceId            = os.Getenv("SDK_INSTANCE_ID")
	featureDBProjectName  = "fdb_test_case"
	featureDBProjectName2 = "featuredb_p3"
	featureDBUserName     = os.Getenv("FEATUREDB_USERNAME")
	featureDBPassword     = os.Getenv("FEATUREDB_PASSWORD")
)

var featuredbFsClient *featurestore.FeatureStoreClient
var featuredbFsClient2 *featurestore.FeatureStoreClient

func initClient(region, projectName string) (*featurestore.FeatureStoreClient, error) {

	accessId := os.Getenv("ACCESS_ID")
	accessKey := os.Getenv("ACCESS_KEY")
	fsclient, err := featurestore.NewFeatureStoreClient(region, accessId, accessKey, projectName,
		featurestore.WithFeatureDBLogin(featureDBUserName, featureDBPassword), featurestore.WithTestMode())
	if err != nil {
		return fsclient, err
	}

	return fsclient, nil
}

func getFeatureDBFsClient() *featurestore.FeatureStoreClient {
	if featuredbFsClient == nil {
		var err error
		featuredbFsClient, err = initClient(regionId, featureDBProjectName)
		if err != nil {
			panic(err)
		}
	}
	return featuredbFsClient
}

func getFeatureDBFsClient2() *featurestore.FeatureStoreClient {
	if featuredbFsClient2 == nil {
		var err error
		featuredbFsClient2, err = initClient(regionId, featureDBProjectName2)
		if err != nil {
			panic(err)
		}
	}
	return featuredbFsClient2
}

func TestReadStreamingKVFeatures(t *testing.T) {
	// 配置运行参数
	featureViewName := "stream_test"
	joinIdName := "user_id"

	// 设置要查询的 JoinId 的值
	joinIds := []interface{}{}
	for i := 0; i < 10; i++ {
		joinIds = append(joinIds, i)
	}

	// 写入数据，数据会落到csv
	featureNameMap := map[string]constants.FSType{
		"f1": constants.FS_INT64,
		"f2": constants.FS_DOUBLE,
		"f3": constants.FS_STRING,
	}
	fileName, err := common.WriteKVFeaturesRandomly(regionId, instanceId, featureDBProjectName2, featureViewName, featureDBUserName, featureDBPassword, joinIdName, featureNameMap, joinIds, constants2.FullRowWrite)
	if err != nil {
		t.Errorf("WriteKVFeaturesRandomly failed, err: %v", err)
	}
	filePath := fmt.Sprintf("./expected_results/%s", fileName)
	defer os.Remove(filePath)

	time.Sleep(2 * time.Second) // 等待2秒，防止写入小延迟
	// 使用 FeatureStore Go SDK 读取数据
	fsClient := getFeatureDBFsClient2()
	result, err := common.ReadKVFeaturesFromFeatureView(fsClient, featureDBProjectName2, featureViewName, joinIds, []string{"*"})
	if err != nil {
		t.Errorf("Failed to read features from feature view: %v", err)
	}

	// 比较结果是否正确
	correct, err := common.CheckResultsWithFile(joinIdName, result, filePath, false)
	if err != nil {
		t.Errorf("Failed to check results: %v", err)
	}

	if !correct {
		t.Logf("Results: %v", result)
		t.Errorf("Results are not correct")
	}
}

func TestReadAllTypeStreamingKVFeatures(t *testing.T) {
	featureViewName := "stream_type_test"
	joinIdName := "user_id"

	joinIds := []interface{}{}
	for i := 0; i < 10; i++ {
		joinIds = append(joinIds, i)
	}
	// 写入数据，数据落到csv
	featureNameMap := map[string]constants.FSType{
		"field_int32":             constants.FS_INT32,
		"field_int64":             constants.FS_INT64,
		"field_float":             constants.FS_FLOAT,
		"field_double":            constants.FS_DOUBLE,
		"field_string":            constants.FS_STRING,
		"field_bool":              constants.FS_BOOLEAN,
		"field_timestamp":         constants.FS_TIMESTAMP,
		"field_array_int32":       constants.FS_ARRAY_INT32,
		"field_array_string":      constants.FS_ARRAY_INT64,
		"field_array_array_float": constants.FS_ARRAY_ARRAY_FLOAT,
		"field_map_int32_int64":   constants.FS_MAP_INT32_INT64,
		"field_map_int64_string":  constants.FS_MAP_INT64_STRING,
		"field_map_string_double": constants.FS_MAP_STRING_DOUBLE,
		"field_map_string_string": constants.FS_MAP_STRING_STRING,
	}
	fileName, err := common.WriteKVFeaturesRandomly(regionId, instanceId, featureDBProjectName2, featureViewName, featureDBUserName, featureDBPassword, joinIdName, featureNameMap, joinIds, constants2.FullRowWrite)
	if err != nil {
		t.Errorf("WriteKVFeaturesRandomly failed, err: %v", err)
	}
	filePath := fmt.Sprintf("./expected_results/%s", fileName)
	defer os.Remove(filePath)

	time.Sleep(2 * time.Second) // 等待2秒，防止写入小延迟
	fsClient := getFeatureDBFsClient2()
	result, err := common.ReadKVFeaturesFromFeatureView(fsClient, featureDBProjectName2, featureViewName, joinIds, []string{"*"})
	if err != nil {
		t.Errorf("Failed to read features from feature view: %v", err)
	}

	correct, err := common.CheckResultsWithFile(joinIdName, result, filePath, false)
	if err != nil {
		t.Errorf("Failed to check results: %v", err)
	}

	if !correct {
		t.Logf("Results: %v", result)
		t.Errorf("Results are not correct")
	}
}

func TestFeatureDBReadFeatureViewKVFeatures1(t *testing.T) {
	featureViewName := "batch_test"
	joinIdName := "user_id"
	joinIds := []interface{}{"166126426", "144417255", "148966755", "178603626", "125719805", "149824754", "135090620", "146780389", "192444324", "154432004"}
	fsClient := getFeatureDBFsClient()
	features, err := common.ReadKVFeaturesFromFeatureView(fsClient, featureDBProjectName, featureViewName, joinIds, []string{"*"})
	if err != nil {
		t.Errorf("Failed to read features from feature view: %v", err)
	}

	expectedResults := []map[string]interface{}{
		{"user_id": "166126426", "gender": "male", "age": 28, "city": "北京市", "item_cnt": 0, "follow_cnt": 1, "follower_cnt": 0, "register_time": 1743496914, "tags": "3"},
		{"user_id": "144417255", "gender": "male", "age": 28, "city": "无锡市", "item_cnt": 0, "follow_cnt": 0, "follower_cnt": 1, "register_time": 1743474814, "tags": "0"},
		{"user_id": "148966755", "gender": "male", "age": 28, "item_cnt": 0, "follow_cnt": 653, "follower_cnt": 1, "register_time": 1743512453, "tags": "0"},
		{"user_id": "178603626", "gender": "male", "age": 28, "item_cnt": 0, "follow_cnt": 12, "follower_cnt": 0, "register_time": 1743474635, "tags": "0"},
		{"user_id": "125719805", "gender": "male", "age": 28, "city": "北京市", "item_cnt": 0, "follow_cnt": 0, "follower_cnt": 166, "register_time": 1743480665, "tags": "1"},
		{"user_id": "149824754", "gender": "female", "age": 28, "item_cnt": 0, "follow_cnt": 0, "follower_cnt": 0, "register_time": 1743478028, "tags": "0"},
		{"user_id": "135090620", "gender": "male", "age": 28, "city": "上海市", "item_cnt": 1, "follow_cnt": 0, "follower_cnt": 0, "register_time": 1743489024, "tags": "0"},
		{"user_id": "146780389", "gender": "male", "age": 28, "item_cnt": 1, "follow_cnt": 0, "follower_cnt": 4, "register_time": 1743466018, "tags": "0"},
		{"user_id": "192444324", "gender": "female", "age": 28, "item_cnt": 0, "follow_cnt": 0, "follower_cnt": 0, "register_time": 1743520074, "tags": "1"},
		{"user_id": "154432004", "age": 28, "city": "长沙市", "item_cnt": 0, "follow_cnt": 1, "follower_cnt": 1, "register_time": 1743498543, "tags": "0"},
	}
	correct, err := common.CheckResults(joinIdName, features, expectedResults, false)
	if err != nil {
		t.Errorf("Failed to check results: %v", err)
	}

	if !correct {
		t.Logf("Results: %v", features)
		t.Errorf("Results are not correct")
	}
}

func TestFeatureDBReadFeatureViewKVFeatures2(t *testing.T) {
	featureViewName := "batch_test"
	joinIdName := "user_id"

	joinIds := []interface{}{"166126426", "144417255", "148966755", "178603626", "125719805", "149824754", "135090620", "146780389", "192444324", "154432004"}
	fsClient := getFeatureDBFsClient()
	features, err := common.ReadKVFeaturesFromFeatureView(fsClient, featureDBProjectName, featureViewName, joinIds, []string{"*"})
	if err != nil {
		t.Errorf("Failed to read features from feature view: %v", err)
	}

	expectedResultsFilePath := "./expected_results/featuredb_test_result1.csv"
	correct, err := common.CheckResultsWithFile(joinIdName, features, expectedResultsFilePath, false)
	if err != nil {
		t.Errorf("Failed to check results: %v", err)
	}

	if !correct {
		t.Logf("Results: %v", features)
		t.Errorf("Results are not correct")
	}
}

func TestFeatureDBBasicTypeReadBatchData(t *testing.T) {
	featureViewName := "test_0611_user_table"

	joinIds := make([]interface{}, 0)
	for i := 0; i < 10; i++ {
		joinIds = append(joinIds, i+1)
	}

	fsClient := getFeatureDBFsClient()
	datalist, err := common.ReadKVFeaturesFromFeatureView(fsClient, featureDBProjectName, featureViewName, joinIds, []string{"score", "amount", "name", "event", "is_active"})
	if err != nil {
		t.Errorf("Failed to read features from feature view(%s): %v", featureViewName, err)
	}

	isExact, err := common.CheckResultsWithFile("user_id", datalist, "./expected_results/featuredb_basic_type_offline_data.csv", false)
	if err != nil {
		t.Errorf("Failed to check results: %v", err)
	}

	if !isExact {
		t.Logf("Results: %v", datalist)
		t.Errorf("Results are not correct")
	}
}

func TestFeatureDBBasicTypeReadStreamData(t *testing.T) {
	var featureViewName = "test_demo_online_table"
	features := map[string]constants.FSType{
		"int2":    constants.FS_INT64,
		"float1":  constants.FS_FLOAT,
		"double1": constants.FS_DOUBLE,
		"string":  constants.FS_STRING,
		"boolean": constants.FS_BOOLEAN,
		"ts":      constants.FS_TIMESTAMP,
	}
	joinIds := make([]interface{}, 0)
	for i := 0; i < 10; i++ {
		joinIds = append(joinIds, i+1)
	}
	fileName, err2 := common.WriteKVFeaturesRandomly(regionId, instanceId, featureDBProjectName, featureViewName, featureDBUserName, featureDBPassword, "item_id", features, joinIds, constants2.FullRowWrite)
	if err2 != nil {
		t.Errorf("Failed to write features to feature view(%s): %v", featureViewName, err2)
	}
	filePath := fmt.Sprintf("./expected_results/%s", fileName)
	defer os.Remove(filePath)

	time.Sleep(2 * time.Second) // 等待2秒，防止写入小延迟
	fsClient := getFeatureDBFsClient()
	datalist, err := common.ReadKVFeaturesFromFeatureView(fsClient, featureDBProjectName, featureViewName, joinIds, []string{"*"})
	if err != nil {
		t.Errorf("Failed to read features from feature view(%s): %v", featureViewName, err)
	}
	isExact, err := common.CheckResultsWithFile("item_id", datalist, filePath, false)
	if err != nil {
		t.Errorf("Failed to check results: %v", err)
	}

	if !isExact {
		t.Logf("Results: %v", datalist)
		t.Errorf("Results are not correct")
	}
}

func TestFeatureDBComplexArrayTypeReadStreamData(t *testing.T) {
	var featureViewName = "test_demo_online_table2"
	features := map[string]constants.FSType{
		"arr_int":       constants.FS_ARRAY_INT32,
		"arr_int2":      constants.FS_ARRAY_INT64,
		"arr_float":     constants.FS_ARRAY_FLOAT,
		"arr_double":    constants.FS_ARRAY_DOUBLE,
		"arr_string":    constants.FS_ARRAY_STRING,
		"arr_arr_float": constants.FS_ARRAY_ARRAY_FLOAT,
	}
	joinIds := make([]interface{}, 0)
	for i := 0; i < 10; i++ {
		joinIds = append(joinIds, i+1)
	}
	fileName, err := common.WriteKVFeaturesRandomly(regionId, instanceId, featureDBProjectName, featureViewName, featureDBUserName, featureDBPassword, "id", features, joinIds, constants2.FullRowWrite)
	if err != nil {
		t.Errorf("Failed to write features to feature view(%s): %v", featureViewName, err)
	}
	filePath := fmt.Sprintf("./expected_results/%s", fileName)
	defer os.Remove(filePath)

	time.Sleep(2 * time.Second) // 等待2秒，防止写入小延迟
	fsClient := getFeatureDBFsClient()
	datalist, err := common.ReadKVFeaturesFromFeatureView(fsClient, featureDBProjectName, featureViewName, joinIds, []string{"*"})
	if err != nil {
		t.Errorf("Failed to read features from feature view(%s): %v", featureViewName, err)
	}
	isExact, err := common.CheckResultsWithFile("id", datalist, filePath, false)
	if err != nil {
		t.Errorf("Failed to check results: %v", err)
	}

	if !isExact {
		t.Logf("Results: %v", datalist)
		t.Errorf("Results are not correct")
	}
}

func TestFeatureDBComplexMapTypeReadStreamData(t *testing.T) {
	var featureViewName = "test_demo_online_table3"
	features := map[string]constants.FSType{
		"map_int":           constants.FS_MAP_INT32_INT32,
		"map_int2":          constants.FS_MAP_INT32_INT64,
		"map_int_float":     constants.FS_MAP_INT32_FLOAT,
		"map_int_double":    constants.FS_MAP_INT32_DOUBLE,
		"map_int_string":    constants.FS_MAP_INT32_STRING,
		"map_int2_int":      constants.FS_MAP_INT64_INT32,
		"map_int2_int2":     constants.FS_MAP_INT64_INT64,
		"map_int2_float":    constants.FS_MAP_INT64_FLOAT,
		"map_int2_double":   constants.FS_MAP_INT64_DOUBLE,
		"map_int2_string":   constants.FS_MAP_INT64_STRING,
		"map_string_int":    constants.FS_MAP_STRING_INT32,
		"map_string_int2":   constants.FS_MAP_STRING_INT64,
		"map_string_float":  constants.FS_MAP_STRING_FLOAT,
		"map_string_double": constants.FS_MAP_STRING_DOUBLE,
		"map_string_string": constants.FS_MAP_STRING_STRING,
	}
	joinIds := make([]interface{}, 0)
	for i := 0; i < 10; i++ {
		joinIds = append(joinIds, i+1)
	}
	fileName, err := common.WriteKVFeaturesRandomly(regionId, instanceId, featureDBProjectName, featureViewName, featureDBUserName, featureDBPassword, "id", features, joinIds, constants2.FullRowWrite)
	filePath := fmt.Sprintf("./expected_results/%s", fileName)
	defer os.Remove(filePath)

	time.Sleep(2 * time.Second) // 等待2秒，防止写入小延迟
	fsClient := getFeatureDBFsClient()
	datalist, err := common.ReadKVFeaturesFromFeatureView(fsClient, featureDBProjectName, featureViewName, joinIds, []string{"*"})
	if err != nil {
		t.Errorf("Failed to read features from feature view(%s): %v", featureViewName, err)
	}
	isExact, err := common.CheckResultsWithFile("id", datalist, filePath, false)
	if err != nil {
		t.Errorf("Failed to check results: %v", err)
	}

	if !isExact {
		t.Logf("Results: %v", datalist)
		t.Errorf("Results are not correct")
	}
}

func TestFeatureDBReadStreamDataWithEventTime(t *testing.T) {
	var featureViewName = "test_verge_online_table"
	joinIds := make([]interface{}, 0)
	keysExist := make(map[int]struct{})
	for len(joinIds) < 20 {
		key := rand.Intn(1000)
		if _, ok := keysExist[key]; !ok {
			keysExist[key] = struct{}{}
			joinIds = append(joinIds, key)
		}
	}
	features := map[string]constants.FSType{
		"config": constants.FS_STRING,
		"brand":  constants.FS_STRING,
		"ts":     constants.FS_INT64,
	}
	fileName, err := common.WriteKVFeaturesExpireWithEventTime(regionId, instanceId, featureDBProjectName, featureViewName, featureDBUserName, featureDBPassword, "item_id", features, joinIds, 3, constants2.FullRowWrite)
	if err != nil {
		t.Errorf("Failed to write critical data: %v", err)
	}
	filePath := fmt.Sprintf("./expected_results/%s", fileName)
	defer os.Remove(filePath)

	time.Sleep(2 * time.Second) // 等待2秒，防止写入延迟
	fsClient := getFeatureDBFsClient()
	datalist, err := common.ReadKVFeaturesFromFeatureView(fsClient, featureDBProjectName, featureViewName, joinIds, []string{"*"})
	if err != nil {
		t.Errorf("Failed to read features from feature view(%s): %v", featureViewName, err)
	}
	isExact, err := common.CheckResultsWithFile("item_id", datalist, filePath, false)
	if err != nil {
		t.Errorf("Failed to check results: %v", err)
	}
	if !isExact {
		t.Logf("Results: %v", datalist)
		t.Errorf("Results are not correct")
	}

	// 检测过期数据是不是检测不到了
	time.Sleep(10 * time.Second)
	datalist2, err2 := common.ReadKVFeaturesFromFeatureView(fsClient, featureDBProjectName, featureViewName, joinIds, []string{"*"})
	if err2 != nil {
		t.Errorf("Failed to read features from feature view(%s): %v", featureViewName, err2)
	}
	if len(datalist2) != 0 {
		t.Errorf("The data did not expire within the specified time.")
	}
}

func TestFeatureDBReadBehaviorData(t *testing.T) {
	var featureViewName = "behavior_view"

	fsClient := getFeatureDBFsClient()
	datalist, err := common.ReadBehaviorFeaturesFromFeatureView(fsClient, featureDBProjectName, featureViewName, []interface{}{"111287215", "122283542", "130682535"}, []interface{}{"click", "praise", "expr"}, []string{"*"})
	if err != nil {
		t.Errorf("Failed to read features from feature view(%s): %v", featureViewName, err)
	}

	isExact, err := common.CheckBehaviorResultsWithFile("user_id", "event", "item_id", "event_time", datalist, "./expected_results/featuredb_basic_type_behavior_data.csv")
	if err != nil {
		t.Errorf("Failed to check results: %v", err)
	}

	if !isExact {
		t.Errorf("Results are not correct")
	}

	//完整注册序列特征视图
	completefeatureViewName := "seq_fea"
	datalist2, err := common.ReadBehaviorFeaturesFromFeatureView(fsClient, featureDBProjectName, completefeatureViewName, []interface{}{"122283542", "111287215", "130682535"}, []interface{}{"click", "praise", "expr"}, []string{"*"})
	if err != nil {
		t.Errorf("Failed to read features from feature view(%s): %v", completefeatureViewName, err)
	}
	isExact, err = common.CheckBehaviorResultsWithFile("user_id", "event", "item_id", "event_time", datalist2, "./expected_results/featuredb_basic_type_behavior_data.csv")
	if err != nil {
		t.Errorf("Failed to check results: %v", err)
	}
	if !isExact {
		//t.Logf("Results: %v", datalist2)
		t.Errorf("Results are not correct")
	}
}

func TestFeatureDBReadSequenceData(t *testing.T) {
	var featureViewName = "seq_fea"
	joinIds := make([]interface{}, 0)
	for i := 0; i < 100; i++ {
		joinIds = append(joinIds, i+1)
	}

	fsClient := getFeatureDBFsClient()
	datalist, err := common.ReadSeqFeaturesFromFeatureView(fsClient, featureDBProjectName, featureViewName, []interface{}{"122283542", "111287215", "118076221", "144744242", "130682535", "102004103"}, []string{"*"})
	if err != nil {
		t.Errorf("Failed to read features from feature view(%s): %v", featureViewName, err)
	}
	isExact, err := common.CheckResultsWithFile("user_id", datalist, "./expected_results/featuredb_basic_type_sequence_data.csv", true)
	if err != nil {
		t.Errorf("Failed to check results: %v", err)
	}

	if !isExact {
		t.Errorf("Results are not correct")
	}

	//reuse sequence feature view
	//完整注册序列特征视图
	completefeatureViewName := "seq_fea_reuse_already_behavior"
	datalist2, err := common.ReadSeqFeaturesFromFeatureView(fsClient, featureDBProjectName, completefeatureViewName, []interface{}{"122283542", "111287215", "118076221", "144744242", "130682535", "102004103"}, []string{"*"})
	isExact, err = common.CheckResultsWithFile("user_id", datalist2, "./expected_results/featuredb_basic_type_sequence_data.csv", true)
	if err != nil {
		t.Errorf("Failed to check results: %v", err)
	}
	if !isExact {
		t.Logf("Results: %v", datalist2)
		t.Errorf("Results are not correct")
	}
}

func TestFeatureDBReadModelFeatureData(t *testing.T) {
	var modelFeatureName = "model_v1"

	fsClient := getFeatureDBFsClient()
	datalist, err := common.ReadFeaturesFromModelFeature(fsClient, featureDBProjectName, modelFeatureName, map[string][]interface{}{"user_id": {"112212303", "165925433"}, "item_id": {"299548114", "271835890"}})
	if err != nil {
		t.Errorf("Failed to read features from feature view(%s): %v", modelFeatureName, err)
	}

	isExact, err := common.CheckResultsWithFile("user_id", datalist, "./expected_results/featuredb_modelfeature_data.csv", true)
	if err != nil {
		t.Errorf("Failed to check results: %v", err)
	}

	if !isExact {
		t.Errorf("Results are not correct")
	}

	//单entity
	datalist2, err := common.ReadFeaturesFromModelFeatureWithFeatureEntity(fsClient, featureDBProjectName, modelFeatureName, map[string][]interface{}{"user_id": {"112212303", "165925433"}, "item_id": {"299548114", "271835890"}}, "user")
	if err != nil {
		t.Errorf("Failed to read modelfeature data(%s): %v", modelFeatureName, err)
	}
	isExact, err = common.CheckResultsWithFile("user_id", datalist2, "./expected_results/featuredb_modelfeature_data.csv", true)
	if err != nil {
		t.Errorf("Failed to check results: %v", err)
	}
	if !isExact {
		t.Logf("Results: %v", datalist2)
		t.Errorf("Results are not correct")
	}
}

func TestFeatureDBPartialFieldData(t *testing.T) {
	featureViewName := "test_partial_online_table"
	joinIds := make([]interface{}, 0)
	for i := 100; i <= 110; i++ {
		joinIds = append(joinIds, i)
	}
	//PartialFieldWrite
	featureNameMap := map[string]constants.FSType{
		"age":    constants.FS_INT32,
		"gender": constants.FS_STRING,
	}

	fileName, err := common.WriteKVFeaturesRandomly(regionId, instanceId, featureDBProjectName, featureViewName, featureDBUserName, featureDBPassword, "user_id", featureNameMap, joinIds, constants2.PartialFieldWrite)
	if err != nil {
		t.Errorf("WriteKVFeaturesRandomly Failed: %v", err)
	}
	filePath := fmt.Sprintf("./expected_results/%s", fileName)
	defer os.Remove(filePath)

	time.Sleep(2 * time.Second) // 等待2秒，防止写入小延迟
	fsClient := getFeatureDBFsClient()
	datalist, err := common.ReadKVFeaturesFromFeatureView(fsClient, featureDBProjectName, featureViewName, joinIds, []string{"age", "gender"})
	if err != nil {
		t.Errorf("Failed to read features from feature view(%s): %v", featureViewName, err)
	}
	isExact, err := common.CheckResultsWithFile("user_id", datalist, filePath, false)
	if err != nil {
		t.Errorf("Failed to check results: %v", err)
	}

	if !isExact {
		t.Logf("Results: %v", datalist)
		t.Errorf("Results are not correct")
	}

	featureNameMap["device_type"] = constants.FS_STRING
	featureNameMap["last_login_time"] = constants.FS_INT64
	fileName, err = common.WriteKVFeaturesRandomly(regionId, instanceId, featureDBProjectName, featureViewName, featureDBUserName, featureDBPassword, "user_id", featureNameMap, joinIds, constants2.FullRowWrite)
	if err != nil {
		t.Errorf("WriteKVFeaturesExpireWithEventTime Failed: %v", err)
	}
	filePath2 := fmt.Sprintf("./expected_results/%s", fileName)
	defer os.Remove(filePath2)

	time.Sleep(2 * time.Second) // 等待2秒，防止写入小延迟
	datalist2, err := common.ReadKVFeaturesFromFeatureView(fsClient, featureDBProjectName, featureViewName, joinIds, []string{"age", "gender", "device_type", "last_login_time"})
	if err != nil {
		t.Errorf("Failed to read features from feature view(%s): %v", featureViewName, err)
	}
	isExact2, err := common.CheckResultsWithFile("user_id", datalist2, filePath2, false)
	if err != nil {
		t.Errorf("Failed to check results: %v", err)
	}
	if !isExact2 {
		t.Logf("Results: %v", datalist2)
		t.Errorf("Results are not correct")
	}
}
