package domain

import "github.com/aliyun/aliyun-pai-featurestore-go-sdk/swagger"

type FeatureEntity struct {
	*swagger.FeatureEntity
}

func NewFeatureEntity(entity swagger.FeatureEntity) *FeatureEntity {
	featureEntity := &FeatureEntity{
		FeatureEntity: &entity,
	}
	return featureEntity
}
