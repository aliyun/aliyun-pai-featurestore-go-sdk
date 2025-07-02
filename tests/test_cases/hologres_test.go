package testcases

import "github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/featurestore"

var (
	hologresProjectName = "holo_test_case"
)

var hologresFsClient *featurestore.FeatureStoreClient

func getHologresFsClient() *featurestore.FeatureStoreClient {
	if hologresFsClient == nil {
		var err error
		hologresFsClient, err = initClient(regionId, hologresProjectName)
		if err != nil {
			panic(err)
		}
	}
	return hologresFsClient
}
