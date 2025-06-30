package common

import (
	"errors"

	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/featurestore"
)

func ReadKVFeaturesFromFeatureView(client *featurestore.FeatureStoreClient, projectName, featureViewName string, joinIds []interface{}, features []string) ([]map[string]interface{}, error) {
	project, err := client.GetProject(projectName)
	if err != nil {
		return nil, err
	}
	featureView := project.GetFeatureView(featureViewName)
	if featureView == nil {
		return nil, errors.New("feature view not found")
	}
	if featureView.GetType() == "Sequence" {
		return nil, errors.New("Please use ReadSeqFeaturesFromFeatureView or ReadBehaviorFeaturesFromFeatureView")
	}

	results, err := featureView.GetOnlineFeatures(joinIds, features, nil)
	if err != nil {
		return nil, err
	}
	return results, nil
}

func ReadSeqFeaturesFromFeatureView(client *featurestore.FeatureStoreClient, projectName, featureViewName string, userIds []interface{}, features []string) ([]map[string]interface{}, error) {
	project, err := client.GetProject(projectName)
	if err != nil {
		return nil, err
	}
	featureView := project.GetFeatureView(featureViewName)
	if featureView == nil {
		return nil, errors.New("feature view not found")
	}
	if featureView.GetType() != "Sequence" {
		return nil, errors.New("Please use ReadKVFeaturesFromFeatureView")
	}

	return featureView.GetOnlineFeatures(userIds, features, nil)
}

func ReadBehaviorFeaturesFromFeatureView(client *featurestore.FeatureStoreClient, projectName, featureViewName string, userIds, events []interface{}, features []string) ([]map[string]interface{}, error) {
	project, err := client.GetProject(projectName)
	if err != nil {
		return nil, err
	}
	featureView := project.GetFeatureView(featureViewName)
	if featureView == nil {
		return nil, errors.New("feature view not found")
	}
	if featureView.GetType() != "Sequence" {
		return nil, errors.New("Please use ReadKVFeaturesFromFeatureView")
	}

	return featureView.GetBehaviorFeatures(userIds, events, features)
}

func ReadFeaturesFromModelFeature(client *featurestore.FeatureStoreClient, projectName, modelFeatureName string, joinIds map[string][]interface{}) ([]map[string]interface{}, error) {
	project, err := client.GetProject(projectName)
	if err != nil {
		return nil, err
	}
	modelFeature := project.GetModelFeature(modelFeatureName)
	if modelFeature == nil {
		return nil, errors.New("model feature not found")
	}

	return modelFeature.GetOnlineFeatures(joinIds)
}

func ReadFeaturesFromModelFeatureWithFeatureEntity(client *featurestore.FeatureStoreClient, projectName, modelFeatureName string, joinIds map[string][]interface{}, featureEntityName string) ([]map[string]interface{}, error) {
	project, err := client.GetProject(projectName)
	if err != nil {
		return nil, err
	}
	modelFeature := project.GetModelFeature(modelFeatureName)
	if modelFeature == nil {
		return nil, errors.New("model feature not found")
	}

	return modelFeature.GetOnlineFeaturesWithEntity(joinIds, featureEntityName)
}
