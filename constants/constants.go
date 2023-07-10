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
)
