package testcases

import "github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/featurestore"

var (
	tablestoreProjectName = "ots_test_case"
)

var tablestoreFsClient *featurestore.FeatureStoreClient

func getTablestoreFsClient() *featurestore.FeatureStoreClient {
	if tablestoreFsClient == nil {
		var err error
		tablestoreFsClient, err = initClient(regionId, tablestoreProjectName)
		if err != nil {
			panic(err)
		}
	}
	return tablestoreFsClient
}
