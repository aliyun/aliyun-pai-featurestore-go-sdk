package featurestore

import (
	"fmt"
	"os"
	"testing"

	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/datasource/featuredb/fdbserverpb"
)

func createFeatureSotreClient() (*FeatureStoreClient, error) {
	accessId := os.Getenv("ALIBABA_CLOUD_ACCESS_KEY_ID")
	accessKey := os.Getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET")

	fdbUser := os.Getenv("FEATUREDB_USERNAME")
	fdbPassword := os.Getenv("FEATUREDB_PASSWORD")

	return NewFeatureStoreClient("cn-beijing", accessId, accessKey, "fs_demo_featuredb", WithDomain("paifeaturestore.cn-beijing.aliyuncs.com"),
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
	features, err := seq_feature_view.GetOnlineFeatures([]interface{}{"199636459"}, []string{"*"}, nil)

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
	project, err := client.GetProject("fs_demo_featuredb")
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
	project, err := client.GetProject("fs_demo_featuredb")
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
