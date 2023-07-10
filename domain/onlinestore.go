package domain

type OnlineStore interface {
	GetTableName(featureView *FeatureView) string
	GetDatasourceName() string
}
