package featurestore

import (
	"fmt"
	"os"
	"testing"

	"fortio.org/assert"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/dao"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/datasource/featuredb/fdbserverpb"
	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/ast"
)

func createFeatureSotreClient() (*FeatureStoreClient, error) {
	accessId := os.Getenv("ALIBABA_CLOUD_ACCESS_KEY_ID")
	accessKey := os.Getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET")

	fdbUser := os.Getenv("FEATUREDB_USERNAME")
	fdbPassword := os.Getenv("FEATUREDB_PASSWORD")

	return NewFeatureStoreClient("cn-shenzhen", accessId, accessKey, "fdb_test", WithDomain("paifeaturestore.cn-shenzhen.aliyuncs.com"),
		WithTestMode(), WithFeatureDBLogin(fdbUser, fdbPassword))

}

func TestGetFeatureViewOnlineFeatures(t *testing.T) {

	// init client
	client, err := createFeatureSotreClient()
	if err != nil {
		t.Fatal(err)
	}

	// get project by name
	project, err := client.GetProject("fs_demo2")
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

func TestGetModelFeatureOnlineFeatures(t *testing.T) {

	// init client
	client, err := createFeatureSotreClient()
	if err != nil {
		t.Fatal(err)
	}

	// get project by name
	project, err := client.GetProject("fs_demo2")
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

	// init client
	client, err := createFeatureSotreClient()
	if err != nil {
		t.Fatal(err)
	}

	// get project by name
	project, err := client.GetProject("fs_demo_featuredb")
	if err != nil {
		t.Fatal(err)
	}

	// get featureview by name
	seq_feature_view := project.GetFeatureView("seq_feature_test")
	if seq_feature_view == nil {
		t.Fatal("feature view not exist")
	}

	// get online features
	features, err := seq_feature_view.GetOnlineFeatures([]interface{}{"199636459", "192535056"}, []string{"*"}, nil)

	if err != nil {
		t.Error(err)
	}

	fmt.Println(features)
}
func TestWriteBloomKV(t *testing.T) {
	// init client
	client, err := createFeatureSotreClient()
	if err != nil {
		t.Fatal(err)
	}

	// get project by name
	project, err := client.GetProject("fdb_test")
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
	client, err := createFeatureSotreClient()
	if err != nil {
		t.Fatal(err)
	}

	// get project by name
	project, err := client.GetProject("fdb_test")
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
	client, err := createFeatureSotreClient()
	if err != nil {
		t.Fatal(err)
	}

	// get project by name
	project, err := client.GetProject("fdb_test")
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

func TestGetFeatureViewRowCount(t *testing.T) {

	// init client
	client, err := createFeatureSotreClient()
	if err != nil {
		t.Fatal(err)
	}

	// get project by name
	project, err := client.GetProject("ceci_test2")
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

	// init client
	client, err := createFeatureSotreClient()
	if err != nil {
		t.Fatal(err)
	}

	t.Run("featuredb test", func(t *testing.T) {
		// get project by name
		project, err := client.GetProject("fdb_test")
		if err != nil {
			t.Fatal(err)
		}
		// get featureview by name
		user_feature_view := project.GetFeatureView("user_test_2")
		if user_feature_view == nil {
			t.Fatal("feature view not exist")
		}
		ids, count, err := user_feature_view.RowCountIds("boolean_field==false")
		assert.Equal(t, nil, err)
		t.Log(count, len(ids))

	})

}
