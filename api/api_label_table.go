package api

import (
	"context"
	"strconv"

	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/constants"
)

// Linger please
var (
	_ context.Context
)

type LabelTableApiService service

func (a *LabelTableApiService) GetLabelTableByID(labelTableId string) (GetLabelTableResponse, error) {
	var (
		localVarReturnValue GetLabelTableResponse
	)
	response, err := a.client.GetLabelTable(&a.client.instanceId, &labelTableId)
	if err != nil {
		return localVarReturnValue, err
	}

	ltid, _ := strconv.Atoi(labelTableId)
	labelTable := LabelTable{
		LabelTableId:   ltid,
		Name:           *response.Body.Name,
		DatasourceName: *response.Body.DatasourceName,
	}
	if id, err := strconv.Atoi(*response.Body.ProjectId); err == nil {
		labelTable.ProjectId = id
	}
	if id, err := strconv.Atoi(*response.Body.DatasourceId); err == nil {
		labelTable.DatasourceId = id
	}

	var fields []*LabelTableField
	for i, fieldItem := range response.Body.Fields {
		field := LabelTableField{
			Name:     *fieldItem.Name,
			Position: i + 1,
		}

		switch *fieldItem.Type {
		case "INT32":
			field.Type = constants.FS_INT32
		case "INT64":
			field.Type = constants.FS_INT64
		case "FLOAT":
			field.Type = constants.FS_FLOAT
		case "DOUBLE":
			field.Type = constants.FS_DOUBLE
		case "BOOLEAN":
			field.Type = constants.FS_BOOLEAN
		case "TIMESTAMP":
			field.Type = constants.FS_TIMESTAMP
		case "ARRAY<INT32>":
			field.Type = constants.FS_ARRAY_INT32
		case "ARRAY<INT64>":
			field.Type = constants.FS_ARRAY_INT64
		case "ARRAY<FLOAT>":
			field.Type = constants.FS_ARRAY_FLOAT
		case "ARRAY<DOUBLE>":
			field.Type = constants.FS_ARRAY_DOUBLE
		case "ARRAY<STRING>":
			field.Type = constants.FS_ARRAY_STRING
		case "ARRAY<ARRAY<FLOAT>>":
			field.Type = constants.FS_ARRAY_ARRAY_FLOAT
		case "MAP<INT32,INT32>":
			field.Type = constants.FS_MAP_INT32_INT32
		case "MAP<INT32,INT64>":
			field.Type = constants.FS_MAP_INT32_INT64
		case "MAP<INT32,FLOAT>":
			field.Type = constants.FS_MAP_INT32_FLOAT
		case "MAP<INT32,DOUBLE>":
			field.Type = constants.FS_MAP_INT32_DOUBLE
		case "MAP<INT32,STRING>":
			field.Type = constants.FS_MAP_INT32_STRING
		case "MAP<INT64,INT32>":
			field.Type = constants.FS_MAP_INT64_INT32
		case "MAP<INT64,INT64>":
			field.Type = constants.FS_MAP_INT64_INT64
		case "MAP<INT64,FLOAT>":
			field.Type = constants.FS_MAP_INT64_FLOAT
		case "MAP<INT64,DOUBLE>":
			field.Type = constants.FS_MAP_INT64_DOUBLE
		case "MAP<INT64,STRING>":
			field.Type = constants.FS_MAP_INT64_STRING
		case "MAP<STRING,INT32>":
			field.Type = constants.FS_MAP_STRING_INT32
		case "MAP<STRING,INT64>":
			field.Type = constants.FS_MAP_STRING_INT64
		case "MAP<STRING,FLOAT>":
			field.Type = constants.FS_MAP_STRING_FLOAT
		case "MAP<STRING,DOUBLE>":
			field.Type = constants.FS_MAP_STRING_DOUBLE
		case "MAP<STRING,STRING>":
			field.Type = constants.FS_MAP_STRING_STRING
		default:
			field.Type = constants.FS_STRING
		}

		for _, attr := range fieldItem.Attributes {
			switch *attr {
			case "Partition":
				field.IsPartition = true
			case "FeatureField":
				field.IsFeatureField = true
			case "FeatureGenerationReserveField":
				field.IsFgReserveField = true
			case "EventTime":
				field.IsEventTime = true
			case "LabelField":
				field.IsLabelField = true
			}
		}

		fields = append(fields, &field)
	}

	labelTable.Fields = fields
	localVarReturnValue.LabelTable = &labelTable
	return localVarReturnValue, nil
}
