package featurestore

import (
	"fmt"
	"os"
	"testing"
)

func createFeatureSotreClient() (*FeatureStoreClient, error) {
	accessId := os.Getenv("AccessId")
	accessKey := os.Getenv("AccessKey")

	return NewFeatureStoreClient("cn-beijing", accessId, accessKey, "fs_test_ots", WithDomain("paifeaturestore.cn-beijing.aliyuncs.com"))
}

func TestGetFeatureViewOnlineFeatures(t *testing.T) {

	// init client
	client, err := createFeatureSotreClient()
	if err != nil {
		t.Fatal(err)
	}

	// get project by name
	project, err := client.GetProject("fs_test_ots")
	if err != nil {
		t.Fatal(err)
	}

	// get featureview by name
	user_feature_view := project.GetFeatureView("user_fea")
	if user_feature_view == nil {
		t.Fatal("feature view not exist")
	}

	// get online features
	features, err := user_feature_view.GetOnlineFeatures([]interface{}{"100000676", "100002990"}, []string{"*"}, nil)

	if err != nil {
		t.Error(err)
	}

	fmt.Println(features)
}

func TestGetModelFeatureOnlineFeatures(t *testing.T) {

	// init client
	client, err := createFeatureSotreClient()
	if err != nil {
		t.Fatal(err)
	}

	// get project by name
	project, err := client.GetProject("fs_test_dj")
	if err != nil {
		t.Fatal(err)
	}

	// get ModelFeature by name
	model_feature := project.GetModelFeature("rank")
	if model_feature == nil {
		t.Fatal("model feature not exist")
	}

	// get online features
	features, err := model_feature.GetOnlineFeaturesWithEntity(map[string][]interface{}{"user_id": {"100000676", "100004208"}}, "user")

	if err != nil {
		t.Error(err)
	}

	fmt.Println(features)
}
