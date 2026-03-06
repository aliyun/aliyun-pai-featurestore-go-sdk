package featurestore

import (
	"fmt"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/constants"
	uuid "github.com/satori/go.uuid"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"fortio.org/assert"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/dao"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/datasource/featuredb/fdbserverpb"
	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/ast"
)

func createFeatureStoreClient(region, projectName string) (*FeatureStoreClient, error) {
	accessId := os.Getenv("ALIBABA_CLOUD_ACCESS_KEY_ID")
	accessKey := os.Getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET")

	fdbUser := os.Getenv("FEATUREDB_USERNAME")
	fdbPassword := os.Getenv("FEATUREDB_PASSWORD")

	return NewFeatureStoreClient(region, accessId, accessKey, projectName, WithDomain(fmt.Sprintf("paifeaturestore.%s.aliyuncs.com", region)),
		WithTestMode(), WithFeatureDBLogin(fdbUser, fdbPassword))

}

const (
	region         = "cn-beijing"
	projectName    = "fs_demo2"
	fdbProjectName = "fdb_test"
)

func TestGetFeatureViewOnlineFeatures(t *testing.T) {

	// init client
	client, err := createFeatureStoreClient(region, projectName)
	if err != nil {
		t.Fatal(err)
	}

	// get project by name
	project, err := client.GetProject(projectName)
	if err != nil {
		t.Fatal(err)
	}

	// get featureview by name
	user_feature_view := project.GetFeatureView("user_table_preprocess_all_feature_v1")
	if user_feature_view == nil {
		t.Fatal("feature view not exist")
	}

	// get online features
	features, err := user_feature_view.GetOnlineFeatures([]interface{}{"100000894", "100029312"}, []string{"*"}, nil)

	if err != nil {
		t.Error(err)
	}

	for _, feature := range features {
		fmt.Println(feature)
	}
}

func TestGetSequenceFeatureViewOfSideInfoFeatures(t *testing.T) {
	fsProjectName := "fdb_test_case"
	client, err := createFeatureStoreClient(region, fsProjectName)
	if err != nil {
		t.Fatal(err)
	}

	project, err := client.GetProject(fsProjectName)
	if err != nil {
		t.Fatal(err)
	}
	seq_feature_view := project.GetFeatureView("seq_fea_side_info_test2")
	features, err := seq_feature_view.GetOnlineFeatures([]interface{}{"135313542", "151362919", "160551912"}, []string{"*"}, nil)
	if err != nil {
		t.Error(err)
	}

	for _, feature := range features {
		fmt.Println(feature)
	}
}

func TestGetModelFeatureOnlineFeatures(t *testing.T) {

	// init client
	client, err := createFeatureStoreClient(region, projectName)
	if err != nil {
		t.Fatal(err)
	}

	// get project by name
	project, err := client.GetProject(projectName)
	if err != nil {
		t.Fatal(err)
	}

	// get ModelFeature by name
	model_feature := project.GetModelFeature("fs_rank_v4")
	if model_feature == nil {
		t.Fatal("model feature not exist")
	}

	// get online features
	features, err := model_feature.GetOnlineFeaturesWithEntity(map[string][]interface{}{"user_id": {"100000894", "100029312"}}, "user")

	if err != nil {
		t.Error(err)
	}

	for _, feature := range features {
		fmt.Println(feature)
	}
}

func TestGetSeqFeatureViewOnlineFeatures(t *testing.T) {
	fdbProjectName := "fs_demo_featuredb"
	// init client
	client, err := createFeatureStoreClient(region, fdbProjectName)
	if err != nil {
		t.Fatal(err)
	}

	// get project by name
	project, err := client.GetProject(fdbProjectName)
	if err != nil {
		t.Fatal(err)
	}

	// get featureview by name
	seq_feature_view := project.GetFeatureView("seq_feature_test")
	if seq_feature_view == nil {
		t.Fatal("feature view not exist")
	}

	// get online features
	features, err := seq_feature_view.GetOnlineFeatures([]interface{}{"133741583", "187524585"}, []string{"*"}, nil)

	if err != nil {
		t.Error(err)
	}
	size1 := 0
	for _, feature := range features {
		if feature != nil {
			for k, value := range feature {
				if value != "" && k != "user_id" {
					strs := strings.Split(value.(string), ";")
					fmt.Println(k, strs)
					size1 += len(strs)
					break
				}
			}
		}
	}

	fmt.Println(features)
	result, err := seq_feature_view.GetOnlineAggregatedFeatures([]interface{}{"133741583", "187524585"}, []string{"*"}, nil)

	if err != nil {
		t.Error(err)
	}

	fmt.Println(result)
	size2 := 0
	for k, value := range result {
		if k != "user_id" {
			strs := strings.Split(value.(string), ";")
			fmt.Println(k, strs)
			size2 += len(strs)
			break

		}
	}
	assert.Equal(t, size1, size2)
}
func TestWriteBloomKV(t *testing.T) {
	// init client
	fsProjectName := "fdb_test"
	client, err := createFeatureStoreClient(region, fsProjectName)
	if err != nil {
		t.Fatal(err)
	}

	// get project by name
	project, err := client.GetProject(fsProjectName)
	if err != nil {
		t.Fatal(err)
	}

	featureView := project.GetFeatureView("user_expose")
	if featureView == nil {
		t.Fatal("feature view not exist")
	}

	request := fdbserverpb.BatchWriteKVReqeust{}
	for i := 0; i < 100; i++ {
		request.Kvs = append(request.Kvs, &fdbserverpb.KVData{Key: "106", Value: []byte(fmt.Sprintf("item_%d", i))})
	}
	err = fdbserverpb.BatchWriteBloomKV(project, featureView, &request)
	if err != nil {
		t.Fatal(err)
	}

}

func TestBloomItems(t *testing.T) {
	// init client
	client, err := createFeatureStoreClient(region, fdbProjectName)
	if err != nil {
		t.Fatal(err)
	}

	// get project by name
	project, err := client.GetProject(fdbProjectName)
	if err != nil {
		t.Fatal(err)
	}

	featureView := project.GetFeatureView("user_expose")
	if featureView == nil {
		t.Fatal("feature view not exist")
	}

	request := fdbserverpb.TestBloomItemsRequest{Key: "106"}
	for i := 0; i < 100; i++ {
		request.Items = append(request.Items, fmt.Sprintf("item_%d", i))
	}
	tests, err := fdbserverpb.TestBloomItems(project, featureView, &request)
	if err != nil {
		t.Fatal(err)
	}
	if len(tests) != len(request.Items) {
		t.Fatal("bloom filter test failed")
	}
	for _, test := range tests {
		if !test {
			t.Fatal("bloom filter test failed")
		}
	}
}
func TestDeleteBloomByKey(t *testing.T) {
	// init client
	client, err := createFeatureStoreClient(region, fdbProjectName)
	if err != nil {
		t.Fatal(err)
	}

	// get project by name
	project, err := client.GetProject(fdbProjectName)
	if err != nil {
		t.Fatal(err)
	}

	featureView := project.GetFeatureView("user_expose")
	if featureView == nil {
		t.Fatal("feature view not exist")
	}

	err = fdbserverpb.DeleteBloomByKey(project, featureView, "106")
	if err != nil {
		t.Fatal(err)
	}
}

func TestExpr(t *testing.T) {
	//code := `(age < 30 && (3 <= level < 5) && sex=='male') `
	testcases := []struct {
		code   string
		expect string
	}{
		{
			code:   "metric_value > 6",
			expect: "metric_value > '6'",
		},
		{
			code:   "6 < metric_value ",
			expect: "'6' < metric_value",
		},
		{
			code:   "sex == 'male'",
			expect: "sex = 'male'",
		},
		{
			code:   "metric_value > 6 && sex == 'male'",
			expect: "(metric_value > '6') and (sex = 'male')",
		},
		{
			code:   "metric_value > 6 && sex == 'male' || os != 'ALL'",
			expect: "((metric_value > '6') and (sex = 'male')) or (os != 'ALL')",
		},
		{
			code:   "(metric_value > 6 && sex == 'male') || (os != 'ALL')",
			expect: "((metric_value > '6') and (sex = 'male')) or (os != 'ALL')",
		},
		{
			code:   "(age < 30 && (3 <= level < 5) && sex=='male')",
			expect: "((age < '30') and (('3' <= level) and (level < '5'))) and (sex = 'male')",
		},
	}
	for _, tcase := range testcases {
		program, err := expr.Compile(tcase.code)
		if err != nil {
			t.Fatal(err)
		}
		node := program.Node()
		visitor := &dao.Visitor{}

		ast.Walk(&node, visitor)

		sql := visitor.ConvertToSql(visitor.LastNode)
		fmt.Println(sql)
		if tcase.expect != "" && sql != tcase.expect {
			t.Fatal("create sql error", sql, tcase.expect)
		}
	}
}
func TestExtractVariables(t *testing.T) {
	//code := `(age < 30 && (3 <= level < 5) && sex=='male') `
	testcases := []struct {
		code   string
		expect []string
	}{
		{
			code:   "metric_value > 6",
			expect: []string{"metric_value"},
		},
		{
			code:   "6 < metric_value ",
			expect: []string{"metric_value"},
		},
		{
			code:   "sex == 'male'",
			expect: []string{"sex"},
		},
		{
			code:   "metric_value > 6 && sex == 'male'",
			expect: []string{"metric_value", "sex"},
		},
		{
			code:   "metric_value > 6 && sex == 'male' || os != 'ALL'",
			expect: []string{"metric_value", "os", "sex"},
		},
		{
			code:   "(metric_value > 6 && sex == 'male') || (os != 'ALL')",
			expect: []string{"metric_value", "os", "sex"},
		},
		{
			code:   "(age < 30 && (3 <= level < 5) && sex=='male')",
			expect: []string{"age", "level", "sex"},
		},
	}
	for _, tcase := range testcases {
		params, err := dao.ExtractVariables(tcase.code)
		assert.NoError(t, err)
		assert.Equal(t, params, tcase.expect)
	}
}

func TestGetFeatureViewRowCount(t *testing.T) {
	fsProjectName := "ceci_test2"
	// init client
	client, err := createFeatureStoreClient(region, fsProjectName)
	if err != nil {
		t.Fatal(err)
	}

	// get project by name
	project, err := client.GetProject(fsProjectName)
	if err != nil {
		t.Fatal(err)
	}

	// get featureview by name
	user_feature_view := project.GetFeatureView("mc")
	if user_feature_view == nil {
		t.Fatal("feature view not exist")
	}

	count := user_feature_view.RowCount("age > 30 && city == '北京市'")
	fmt.Println(count)
}

func TestFeatureViewRowIdCount(t *testing.T) {
	fsProjectName := "fs_demo_featuredb"
	// init client
	client, err := createFeatureStoreClient(region, fsProjectName)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("featuredb test", func(t *testing.T) {
		// get project by name
		project, err := client.GetProject(fsProjectName)
		if err != nil {
			t.Fatal(err)
		}
		// get featureview by name
		user_feature_view := project.GetFeatureView("user_test_2")
		if user_feature_view == nil {
			t.Fatal("feature view not exist")
		}
		ids, count1, err := user_feature_view.RowCountIds("int32_field >= 0")
		assert.Equal(t, nil, err)
		assert.Equal(t, count1, len(ids))
		_, count2, _ := user_feature_view.RowCountIds("int32_field < 0") // true

		_, total, _ := user_feature_view.RowCountIds("") // true
		assert.Equal(t, count1+count2, total)
	})

}

func TestScanAndIterateData(t *testing.T) {

	// init client
	client, err := createFeatureStoreClient(region, fdbProjectName)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("no channel", func(t *testing.T) {
		// get project by name
		project, err := client.GetProject(fdbProjectName)
		if err != nil {
			t.Fatal(err)
		}
		// get featureview by name
		user_feature_view := project.GetFeatureView("user_test_2")
		if user_feature_view == nil {
			t.Fatal("feature view not exist")
		}
		ids, err := user_feature_view.ScanAndIterateData("boolean_field==false", nil)
		assert.Equal(t, nil, err)
		t.Log("ids size:", len(ids))
	})
	t.Run("have channel", func(t *testing.T) {
		// get project by name
		project, err := client.GetProject("fdb_test")
		if err != nil {
			t.Fatal(err)
		}
		ch := make(chan string)
		// get featureview by name
		user_feature_view := project.GetFeatureView("user_test_2")
		if user_feature_view == nil {
			t.Fatal("feature view not exist")
		}
		ids, err := user_feature_view.ScanAndIterateData("boolean_field==false", ch)
		assert.Equal(t, nil, err)
		t.Log("ids size:", len(ids))

		i := 0
		for id := range ch {
			t.Log(id)
			i++
			if i > 100 {
				break
			}
		}
	})

}

func transfer(featureNameMap map[string]constants.FSType, joinIds []interface{}, joinIdName string) ([]map[string]interface{}, error) {
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
				return data, fmt.Errorf("unsupported feature type: %v", featureType)
			}
		}
		data = append(data, row)
	}
	return data, nil
}

const (
	projectName2 = "fs_python_test1013"
)

func TestWriteFeaturesToFeatureViewAsync(t *testing.T) {
	client, err := createFeatureStoreClient(region, projectName2)
	if err != nil {
		t.Fatal(err)
	}

	project, err := client.GetProject(projectName2)
	if err != nil {
		t.Fatal(err)
	}

	onlineFeatureView := "test_pro1" //"test0304"
	//onlineFeatureView2 := "complex_features"
	//offlineFeatureView := "feature_view_users"
	featureView := project.GetFeatureView(onlineFeatureView)
	if featureView == nil {
		t.Fatal("feature view not exist")
	}

	writeData := make([]map[string]interface{}, 0, 10)
	joinIds := make([]interface{}, 10)
	for i := 0; i < 10; i++ { // 10 个不同的用户
		int64Seed := rand.Int63n(10000000)
		int32Seed := rand.Int31n(1000)
		float32Seed := rand.Float32()
		float64Seed := rand.Float64()
		joinIds[i] = int64Seed
		record := map[string]interface{}{
			"user_id":       fmt.Sprintf("%d", int64Seed),
			"string_field":  uuid.NewV1().String()[0:8],
			"int32_field":   int32(i) * int32Seed,
			"int64_field":   int64(i) * int64Seed,
			"float_field":   float32(i) * float32Seed,
			"double_field":  float64(i) * float64Seed,
			"boolean_field": i%2 == 0,
		}
		writeData = append(writeData, record)
	}

	//for i := 10; i < 20; i++ {
	//	//online featureView
	//	int32Seed := rand.Int31()
	//	//float64Seed := rand.Float64()
	//	float32Seed := rand.Float32()
	//	//var boolSeed bool
	//	//if i%2 == 0 {
	//	//	boolSeed = true
	//	//} else {
	//	//	boolSeed = false
	//	//}
	//	record := map[string]interface{}{
	//		"a_id": fmt.Sprintf("%d", 185284895+i),
	//		//"b":    int64(23201000 + i), // 10 个不同的用户
	//		//"c":    float64(i) * float64Seed,
	//		//"d":    boolSeed,
	//		"e": float32(i) * float32Seed,
	//		"f": int32(i) * int32Seed,
	//		"g": time.Now().UnixMilli(),
	//	}
	//
	//	//offine featureView
	//	//record := map[string]interface{}{
	//	//	"user_md5":      fmt.Sprintf("%d", 185284895+i),
	//	//	"user_nickname": uuid.NewV1().String()[0:8],
	//	//}
	//
	//	writeData = append(writeData, record)
	//
	//}

	//featuresType := map[string]constants.FSType{
	//	"arr1":  constants.FS_ARRAY_INT32,
	//	"arr2":  constants.FS_ARRAY_INT64,
	//	"arr3":  constants.FS_ARRAY_FLOAT,
	//	"arr4":  constants.FS_ARRAY_DOUBLE,
	//	"arr5":  constants.FS_ARRAY_STRING,
	//	"arr6":  constants.FS_ARRAY_ARRAY_FLOAT,
	//	"map1":  constants.FS_MAP_INT32_INT32,
	//	"map2":  constants.FS_MAP_INT32_INT64,
	//	"map3":  constants.FS_MAP_INT32_FLOAT,
	//	"map4":  constants.FS_MAP_INT32_DOUBLE,
	//	"map5":  constants.FS_MAP_INT32_STRING,
	//	"map6":  constants.FS_MAP_INT64_INT32,
	//	"map7":  constants.FS_MAP_INT64_INT64,
	//	"map8":  constants.FS_MAP_INT32_FLOAT,
	//	"map9":  constants.FS_MAP_INT32_DOUBLE,
	//	"map10": constants.FS_MAP_INT32_STRING,
	//	"map11": constants.FS_MAP_INT64_INT32,
	//	"map12": constants.FS_MAP_INT64_INT64,
	//	"map13": constants.FS_MAP_INT64_FLOAT,
	//	"map14": constants.FS_MAP_INT64_DOUBLE,
	//	"map15": constants.FS_MAP_INT64_STRING,
	//	"map16": constants.FS_MAP_STRING_INT32,
	//	"map17": constants.FS_MAP_STRING_INT64,
	//	"map18": constants.FS_MAP_STRING_FLOAT,
	//	"map19": constants.FS_MAP_STRING_DOUBLE,
	//	"map20": constants.FS_MAP_STRING_STRING,
	//}

	//joinIds := []interface{}{"185284895", "185284896", "185284897", "185284898", "185284899"}
	//writeData, err = transfer(featuresType, joinIds, "m_id")
	if err != nil {
		t.Fatal(err)
	}

	featureView.WriteFeatures(writeData)
	//featureView.WriteFeaturesWithInsertMode(writeData, constants.PartialFieldWrite)
	featureView.WriteFlush()

	time.Sleep(3 * time.Second)

	//features, err := featureView.GetOnlineFeatures([]interface{}{"185284905", "185284906", "185284907", "185284908", "185284909"}, []string{"*"}, nil)
	features, err := featureView.GetOnlineFeatures(joinIds, []string{"*"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	if len(features) == 0 {
		t.Fatal("get online feature none")
	}

	for _, feature := range features {
		fmt.Println(feature)
	}
}

func TestWriteFeaturesToSequenceFeatureViewAsync(t *testing.T) {

	// init client
	client, err := createFeatureStoreClient(region, projectName2)
	if err != nil {
		t.Fatal(err)
	}

	// get project by name
	project, err := client.GetProject(projectName2)
	if err != nil {
		t.Fatal(err)
	}

	// get featureview by name
	featureView := project.GetFeatureView("seq_test60")
	if featureView == nil {
		t.Fatal("feature view not exist")
	}

	joinIds := []interface{}{int64(185284895), int64(185284896), int64(185284897), int64(185284898), int64(185284899)}

	recordsPerUser := 10 // 每个用户 10 条记录
	writeData := make([]map[string]interface{}, 0, len(joinIds)*recordsPerUser)

	events := []string{"click", "expr"}
	pages := []string{"home_page", "detail_page", "list_page", "search_page"}
	netTypes := []string{"wifi", "4g", "5g"}

	for _, joinId := range joinIds {
		baseTime := time.Now().Add(-time.Duration(len(joinIds)*recordsPerUser) * time.Minute)

		for i := 0; i < recordsPerUser; i++ {
			row := make(map[string]interface{})

			// ✅ 主键设置为 int64（同一个用户）
			row["user_id"] = joinId

			// ✅ 其他字段随机生成（每条记录不同）
			row["request_id"] = int64(rand.Intn(1000000))
			row["exp_id"] = fmt.Sprintf("exp_%d", rand.Intn(100))
			row["page"] = pages[rand.Intn(len(pages))]
			row["net_type"] = netTypes[rand.Intn(len(netTypes))]

			// ✅ 时间递增（模拟真实行为序列）
			eventTime := baseTime.Add(time.Duration(i) * time.Minute)
			row["event_time"] = eventTime.UnixMilli()

			row["item_id"] = int64(800000 + rand.Intn(10000))
			row["event"] = events[rand.Intn(len(events))]
			row["playtime"] = rand.Float64() * 100.0

			writeData = append(writeData, row)
		}
	}

	featureView.WriteFeatures(writeData)
	//featureView.WriteFeaturesWithInsertMode(writeData, constants.PartialFieldWrite)
	featureView.WriteFlush()

	// 等待数据写入完成（实际场景中应该由业务逻辑控制何时 flush）
	time.Sleep(3 * time.Second)
	features, err := featureView.GetOnlineFeatures([]interface{}{185284895, 185284896, 185284897, 185284898, 185284899}, []string{"*"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	if len(features) == 0 {
		t.Error("Expected to read some features, but got none")
	}

	for _, feature := range features {
		fmt.Printf("Feature: %v\n", feature)
	}

}
