package featurestore

import (
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"fortio.org/assert"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/constants"
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

const (
	projectName2 = "fs_demo_featuredb"
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

	onlineFeatureView := "user_test_2" //"test_pro1"
	//onlineFeatureView2 := "complex_features"
	//offlineFeatureView := "feature_view_users"
	featureView := project.GetFeatureView(onlineFeatureView)
	if featureView == nil {
		t.Fatal("feature view not exist")
	}

	writeData := make([]map[string]interface{}, 0, 10)

	for i := 10; i < 20; i++ {
		var boolSeed bool
		if i%2 == 0 {
			boolSeed = true
		} else {
			boolSeed = false
		}
		record := map[string]interface{}{
			"user_id":       int64(185284895 + i),
			"string_field":  fmt.Sprintf("test_str_%d", i),
			"int32_field":   int32(i) * rand.Int31n(100),
			"int64_field":   int64(i) * rand.Int63n(10000),
			"float_field":   float32(i) * rand.Float32(),
			"double_field":  float64(i) * rand.Float64(),
			"boolean_field": boolSeed,
		}

		writeData = append(writeData, record)
	}

	featureView.WriteFeatures(writeData)
	//featureView.WriteFeaturesWithInsertMode(writeData, constants.PartialFieldWrite)
	featureView.WriteFlush()

	time.Sleep(3 * time.Second)

	features, err := featureView.GetOnlineFeatures([]interface{}{int64(185284905), int64(185284906), int64(185284907), int64(185284908), int64(185284909)}, []string{"*"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	if len(features) == 0 {
		t.Fatal("get online feature none")
	}

	// build a lookup map from writeData by user_id
	expectedMap := make(map[int64]map[string]interface{})
	for _, record := range writeData {
		expectedMap[record["user_id"].(int64)] = record
	}

	for _, feature := range features {
		fmt.Println(feature)

		userID, ok := feature["user_id"]
		if !ok {
			t.Fatal("missing user_id field")
		}
		uid, ok := userID.(int64)
		if !ok {
			t.Fatalf("user_id expected int64, got %T", userID)
		}

		expected, exists := expectedMap[uid]
		if !exists {
			t.Fatalf("unexpected user_id %d in results", uid)
		}

		if v, ok := feature["string_field"]; ok {
			if v != expected["string_field"] {
				t.Fatalf("user_id=%d string_field mismatch: got %v, want %v", uid, v, expected["string_field"])
			}
		}

		if v, ok := feature["int32_field"]; ok {
			if v != expected["int32_field"] {
				t.Fatalf("user_id=%d int32_field mismatch: got %v, want %v", uid, v, expected["int32_field"])
			}
		}

		if v, ok := feature["int64_field"]; ok {
			if v != expected["int64_field"] {
				t.Fatalf("user_id=%d int64_field mismatch: got %v, want %v", uid, v, expected["int64_field"])
			}
		}

		if v, ok := feature["float_field"]; ok {
			if v != expected["float_field"] {
				t.Fatalf("user_id=%d float_field mismatch: got %v, want %v", uid, v, expected["float_field"])
			}
		}

		if v, ok := feature["double_field"]; ok {
			if v != expected["double_field"] {
				t.Fatalf("user_id=%d double_field mismatch: got %v, want %v", uid, v, expected["double_field"])
			}
		}

		if v, ok := feature["boolean_field"]; ok {
			if v != expected["boolean_field"] {
				t.Fatalf("user_id=%d boolean_field mismatch: got %v, want %v", uid, v, expected["boolean_field"])
			}
		}
	}
}

func TestWriteFeaturesToFeatureViewWithPartialFieldWrite(t *testing.T) {

	onlineFeatureView := "user_test_3"

	var fullWriteData []map[string]interface{}

	t.Run("FullRowWrite", func(t *testing.T) {
		client, err := createFeatureStoreClient(region, projectName2)
		if err != nil {
			t.Fatal(err)
		}

		project, err := client.GetProject(projectName2)
		if err != nil {
			t.Fatal(err)
		}
		featureView := project.GetFeatureView(onlineFeatureView)
		if featureView == nil {
			t.Fatal("feature view not exist")
		}

		fullWriteData = make([]map[string]interface{}, 0, 5)
		for i := 10; i < 15; i++ {
			var boolSeed bool
			if i%2 == 0 {
				boolSeed = true
			} else {
				boolSeed = false
			}
			record := map[string]interface{}{
				"user_id":       int64(185284895 + i),
				"string_field":  fmt.Sprintf("full_str_%d", i),
				"int32_field":   int32(i * 100),
				"int64_field":   int64(i * 1000),
				"float_field":   float32(i) * 1.5,
				"double_field":  float64(i) * 2.5,
				"boolean_field": boolSeed,
			}
			fullWriteData = append(fullWriteData, record)
		}

		featureView.WriteFeatures(fullWriteData)
		featureView.WriteFlush()
		time.Sleep(3 * time.Second)
	})

	t.Run("PartialFieldWrite", func(t *testing.T) {
		client, err := createFeatureStoreClient(region, projectName2)
		if err != nil {
			t.Fatal(err)
		}

		project, err := client.GetProject(projectName2)
		if err != nil {
			t.Fatal(err)
		}
		featureView := project.GetFeatureView(onlineFeatureView)
		if featureView == nil {
			t.Fatal("feature view not exist")
		}

		partialWriteData := make([]map[string]interface{}, 0, 5)
		for i := 10; i < 15; i++ {
			record := map[string]interface{}{
				"user_id":      int64(185284895 + i),
				"string_field": fmt.Sprintf("partial_str_%d", i),
				"int32_field":  int32(i * 999),
			}
			partialWriteData = append(partialWriteData, record)
		}

		featureView.WriteFeaturesWithInsertMode(partialWriteData, constants.PartialFieldWrite)
		featureView.WriteFlush()
		time.Sleep(3 * time.Second)

		// read and verify
		features, err := featureView.GetOnlineFeatures([]interface{}{int64(185284905), int64(185284906), int64(185284907), int64(185284908), int64(185284909)}, []string{"*"}, nil)
		if err != nil {
			t.Fatal(err)
		}

		if len(features) == 0 {
			t.Fatal("get online feature none")
		}

		// build expected: full write as base, then overlay partial write fields
		expectedMap := make(map[int64]map[string]interface{})
		for _, record := range fullWriteData {
			expectedMap[record["user_id"].(int64)] = record
		}
		for _, record := range partialWriteData {
			uid := record["user_id"].(int64)
			for k, v := range record {
				expectedMap[uid][k] = v
			}
		}

		for _, feature := range features {
			fmt.Println(feature)

			userID, ok := feature["user_id"]
			if !ok {
				t.Fatal("missing user_id field")
			}
			uid, ok := userID.(int64)
			if !ok {
				t.Fatalf("user_id expected int64, got %T", userID)
			}

			expected, exists := expectedMap[uid]
			if !exists {
				t.Fatalf("unexpected user_id %d in results", uid)
			}

			// verify partially written fields have new values
			if v, ok := feature["string_field"]; ok {
				if v != expected["string_field"] {
					t.Fatalf("user_id=%d string_field mismatch: got %v, want %v", uid, v, expected["string_field"])
				}
			}
			if v, ok := feature["int32_field"]; ok {
				if v != expected["int32_field"] {
					t.Fatalf("user_id=%d int32_field mismatch: got %v, want %v", uid, v, expected["int32_field"])
				}
			}

			// verify non-updated fields are preserved from full write
			if v, ok := feature["int64_field"]; ok {
				if v != expected["int64_field"] {
					t.Fatalf("user_id=%d int64_field mismatch: got %v, want %v", uid, v, expected["int64_field"])
				}
			} else {
				t.Fatalf("user_id=%d int64_field missing after partial write, expected preserved", uid)
			}
			if v, ok := feature["float_field"]; ok {
				if v != expected["float_field"] {
					t.Fatalf("user_id=%d float_field mismatch: got %v, want %v", uid, v, expected["float_field"])
				}
			} else {
				t.Fatalf("user_id=%d float_field missing after partial write, expected preserved", uid)
			}
			if v, ok := feature["double_field"]; ok {
				if v != expected["double_field"] {
					t.Fatalf("user_id=%d double_field mismatch: got %v, want %v", uid, v, expected["double_field"])
				}
			} else {
				t.Fatalf("user_id=%d double_field missing after partial write, expected preserved", uid)
			}
			if v, ok := feature["boolean_field"]; ok {
				if v != expected["boolean_field"] {
					t.Fatalf("user_id=%d boolean_field mismatch: got %v, want %v", uid, v, expected["boolean_field"])
				}
			} else {
				t.Fatalf("user_id=%d boolean_field missing after partial write, expected preserved", uid)
			}
		}
	})
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
	featureView := project.GetFeatureView("seq_feature_test")
	if featureView == nil {
		t.Fatal("feature view not exist")
	}

	joinIds := []interface{}{"185284895", "185284896", "185284897", "185284898", "185284899"}

	recordsPerUser := 10 // 每个用户 10 条记录
	writeData := make([]map[string]interface{}, 0, len(joinIds)*recordsPerUser)

	events := []string{"click", "expr"}
	pages := []string{"home_page", "detail_page", "list_page", "search_page"}
	netTypes := []string{"wifi", "4g", "5g"}

	for _, joinId := range joinIds {
		baseTime := time.Now().Add(-time.Duration(len(joinIds)*recordsPerUser) * time.Minute)

		for i := 0; i < recordsPerUser; i++ {
			row := make(map[string]interface{})

			row["user_id"] = joinId

			row["request_id"] = int64(rand.Intn(1000000))
			row["page"] = pages[rand.Intn(len(pages))]
			row["net_type"] = netTypes[rand.Intn(len(netTypes))]

			eventTime := baseTime.Add(time.Duration(i) * time.Minute)
			row["event_unix_time"] = eventTime.UnixMilli()

			row["item_id"] = fmt.Sprintf("%d", 800000+rand.Intn(10000))
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
	features, err := featureView.GetOnlineFeatures(joinIds, []string{"*"}, nil)
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
