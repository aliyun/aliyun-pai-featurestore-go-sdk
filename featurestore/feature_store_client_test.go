package featurestore

import (
	"fmt"
	"os"
	"strings"
	"testing"

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
