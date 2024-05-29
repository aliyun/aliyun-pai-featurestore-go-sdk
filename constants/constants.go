package constants

type FSType int

const (
	FS_INT32 FSType = iota + 1 // int32
	FS_INT64                   // int64
	FS_FLOAT
	FS_DOUBLE
	FS_STRING
	FS_BOOLEAN
	FS_TIMESTAMP
	FS_ARRAY_INT32
	FS_ARRAY_INT64
	FS_ARRAY_FLOAT
	FS_ARRAY_DOUBLE
	FS_ARRAY_STRING
	FS_ARRAY_ARRAY_FLOAT
	FS_MAP_INT32_INT32
	FS_MAP_INT32_INT64
	FS_MAP_INT32_FLOAT
	FS_MAP_INT32_DOUBLE
	FS_MAP_INT32_STRING
	FS_MAP_INT64_INT32
	FS_MAP_INT64_INT64
	FS_MAP_INT64_FLOAT
	FS_MAP_INT64_DOUBLE
	FS_MAP_INT64_STRING
	FS_MAP_STRING_INT32
	FS_MAP_STRING_INT64
	FS_MAP_STRING_FLOAT
	FS_MAP_STRING_DOUBLE
	FS_MAP_STRING_STRING
)
const (
	Datasource_Type_MaxCompute = "maxcompute"
	Datasource_Type_Hologres   = "hologres"
	Datasource_Type_Redis      = "redis"
	Datasource_Type_Mysql      = "mysql"
	Datasource_Type_IGraph     = "igraph"
	Datasource_Type_Spark      = "spark"
	Datasource_Type_TableStore = "tablestore"
	Datasource_Type_FeatureDB  = "featuredb"
)
const (
	Feature_View_Type_Batch    = "Batch"
	Feature_View_Type_Stream   = "Stream"
	Feature_View_Type_Sequence = "Sequence"
)
