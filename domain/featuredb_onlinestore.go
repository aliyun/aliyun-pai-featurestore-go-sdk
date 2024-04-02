package domain

import "github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/api"

type FeatureDBOnlineStore struct {
	*api.Datasource
}

func (s *FeatureDBOnlineStore) GetTableName(featureView *BaseFeatureView) string {
	return featureView.Name
}

func (s *FeatureDBOnlineStore) GetDatasourceName() string {
	return s.Name
}

func (s *FeatureDBOnlineStore) GetSeqOfflineTableName(seqFeatureView *SequenceFeatureView) string {
	return ""
}

func (s *FeatureDBOnlineStore) GetSeqOnlineTableName(sequenceFeatureView *SequenceFeatureView) string {
	return ""
}
