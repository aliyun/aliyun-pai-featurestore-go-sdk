package featurestore

import (
	"fmt"
	"os"
	"testing"
)

func createFeatureSotreClient() (*FeatureStoreClient, error) {
	accessId := os.Getenv("ALIBABA_CLOUD_ACCESS_KEY_ID")
	accessKey := os.Getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET")

	return NewFeatureStoreClient("cn-beijing", accessId, accessKey, "fs_demo2", WithDomain("paifeaturestore.cn-beijing.aliyuncs.com"), WithHologresPort(81), WithTestMode())
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
