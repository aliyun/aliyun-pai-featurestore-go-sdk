package dao

import (
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/constants"
)

type DaoConfig struct {
	DatasourceType string

	PrimaryKeyField string
	EventTimeField  string
	TTL             int

	// hologres
	HologresName      string
	HologresTableName string

	//tablestore
	TableStoreName      string
	TableStoreTableName string

	// igraph
	IGraphName        string
	GroupName         string
	LabelName         string
	SaveOriginalField bool

	FieldMap map[string]string
	// tablestore, featuredb
	FieldTypeMap map[string]constants.FSType

	// featuredb
	Fields []string

	// hologres sequence tables
	HologresOnlineTableName  string
	HologresOfflineTableName string
	// tablestore sequence tables
	TableStoreOnlineTableName  string
	TableStoreOfflineTableName string
	// igraph sequence table
	IgraphEdgeName string

	// featuredb
	FeatureDBDatabaseName string
	FeatureDBSchemaName   string
	FeatureDBTableName    string
	FeatureDBSignature    string
}
