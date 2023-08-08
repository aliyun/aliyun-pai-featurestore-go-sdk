package featurestore

import (
	"fmt"
	"testing"
)

func TestGetOTSFeatures(t *testing.T) {
	host := "http://localhost:8080"

	// init client
	client, err := NewFeatureStoreClient(host, WithToken(""))
	if err != nil {
		t.Error(err)
	}

	// get project by name
	project, err := client.GetProject("rec_ots")
	if err != nil {
		t.Error(err)
	}

	// get featureview by name
	user_feature_view := project.GetFeatureView("user_fea1")
	if user_feature_view == nil {
		t.Fatal("feature view not exist")
	}

	// get online features
	features, err := user_feature_view.GetOnlineFeatures([]interface{}{"100000676", "100001662"}, []string{"*"}, nil)

	if err != nil {
		t.Error(err)
	}

	fmt.Println(features)
}
