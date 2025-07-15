package api

import (
	"context"
	"strconv"

	paifeaturestore "github.com/alibabacloud-go/paifeaturestore-20230621/v4/client"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/constants"
	"github.com/antihax/optional"
)

// Linger please
var (
	_ context.Context
)

type FsModelApiService service

/*
FsModelApiService Get Model By ID
  - @param ctx context.Context - for authentication, logging, cancellation, deadlines, tracing, etc. Passed from http.Request or context.Background().
  - @param modelId

@return InlineResponse20086
*/
func (a *FsModelApiService) GetModelByID(modelId string) (GetModelResponse, error) {
	var (
		localVarReturnValue GetModelResponse
	)

	response, err := a.client.GetModelFeature(&a.client.instanceId, &modelId)
	if err != nil {
		return localVarReturnValue, err
	}

	mid, _ := strconv.Atoi(modelId)
	model := Model{
		ModelId:              mid,
		ProjectName:          *response.Body.ProjectName,
		Name:                 *response.Body.Name,
		LabelDatasourceTable: *response.Body.LabelTableName,
	}
	if id, err := strconv.Atoi(*response.Body.ProjectId); err == nil {
		model.ProjectId = id
	}
	if id, err := strconv.Atoi(*response.Body.LabelTableId); err == nil {
		model.LabelTableId = id
	}
	if response.Body.LabelPriorityLevel != nil {
		model.LabelPriorityLevel = int(*response.Body.LabelPriorityLevel)
	}

	var features []*ModelFeatures
	for _, featureItem := range response.Body.Features {
		feature := ModelFeatures{
			FeatureViewName: *featureItem.FeatureViewName,
			Name:            *featureItem.Name,
		}
		if featureItem.AliasName != nil && *featureItem.AliasName != "" && *featureItem.AliasName != feature.Name {
			feature.AliasName = *featureItem.AliasName
		}
		if id, err := strconv.Atoi(*featureItem.FeatureViewId); err == nil {
			feature.FeatureViewId = id
		}
		switch *featureItem.Type {
		case "INT32":
			feature.Type = int32(constants.FS_INT32)
		case "INT64":
			feature.Type = int32(constants.FS_INT64)
		case "FLOAT":
			feature.Type = int32(constants.FS_FLOAT)
		case "DOUBLE":
			feature.Type = int32(constants.FS_DOUBLE)
		case "BOOLEAN":
			feature.Type = int32(constants.FS_BOOLEAN)
		case "TIMESTAMP":
			feature.Type = int32(constants.FS_TIMESTAMP)
		case "ARRAY<INT32>":
			feature.Type = int32(constants.FS_ARRAY_INT32)
		case "ARRAY<INT64>":
			feature.Type = int32(constants.FS_ARRAY_INT64)
		case "ARRAY<FLOAT>":
			feature.Type = int32(constants.FS_ARRAY_FLOAT)
		case "ARRAY<DOUBLE>":
			feature.Type = int32(constants.FS_ARRAY_DOUBLE)
		case "ARRAY<STRING>":
			feature.Type = int32(constants.FS_ARRAY_STRING)
		case "ARRAY<ARRAY<FLOAT>>":
			feature.Type = int32(constants.FS_ARRAY_ARRAY_FLOAT)
		case "MAP<INT32,INT32>":
			feature.Type = int32(constants.FS_MAP_INT32_INT32)
		case "MAP<INT32,INT64>":
			feature.Type = int32(constants.FS_MAP_INT32_INT64)
		case "MAP<INT32,FLOAT>":
			feature.Type = int32(constants.FS_MAP_INT32_FLOAT)
		case "MAP<INT32,DOUBLE>":
			feature.Type = int32(constants.FS_MAP_INT32_DOUBLE)
		case "MAP<INT32,STRING>":
			feature.Type = int32(constants.FS_MAP_INT32_STRING)
		case "MAP<INT64,INT32>":
			feature.Type = int32(constants.FS_MAP_INT64_INT32)
		case "MAP<INT64,INT64>":
			feature.Type = int32(constants.FS_MAP_INT64_INT64)
		case "MAP<INT64,FLOAT>":
			feature.Type = int32(constants.FS_MAP_INT64_FLOAT)
		case "MAP<INT64,DOUBLE>":
			feature.Type = int32(constants.FS_MAP_INT64_DOUBLE)
		case "MAP<INT64,STRING>":
			feature.Type = int32(constants.FS_MAP_INT64_STRING)
		case "MAP<STRING,INT32>":
			feature.Type = int32(constants.FS_MAP_STRING_INT32)
		case "MAP<STRING,INT64>":
			feature.Type = int32(constants.FS_MAP_STRING_INT64)
		case "MAP<STRING,FLOAT>":
			feature.Type = int32(constants.FS_MAP_STRING_FLOAT)
		case "MAP<STRING,DOUBLE>":
			feature.Type = int32(constants.FS_MAP_STRING_DOUBLE)
		case "MAP<STRING,STRING>":
			feature.Type = int32(constants.FS_MAP_STRING_STRING)
		default:
			feature.Type = int32(constants.FS_STRING)
		}

		features = append(features, &feature)
	}

	model.Features = features
	localVarReturnValue.Model = &model
	return localVarReturnValue, nil
}

/*
FsModelApiService List Models
 * @param ctx context.Context - for authentication, logging, cancellation, deadlines, tracing, etc. Passed from http.Request or context.Background().
 * @param optional nil or *FsModelApiListModelsOpts - Optional Parameters:
     * @param "Pagesize" (optional.Int32) -
     * @param "Pagenumber" (optional.Int32) -
     * @param "ProjectId" (optional.Int32) -
@return InlineResponse20085
*/

type FsModelApiListModelsOpts struct {
	Pagesize   optional.Int32
	Pagenumber optional.Int32
	ProjectId  optional.Int32
}

func (a *FsModelApiService) ListModels(pagesize, pagenumber int, projectId string) (ListModelsResponse, error) {
	var (
		localVarReturnValue ListModelsResponse
	)
	request := paifeaturestore.ListModelFeaturesRequest{}
	request.SetPageSize(int32(pagesize))
	request.SetPageNumber(int32(pagenumber))
	request.SetProjectId(projectId)

	response, err := a.client.ListModelFeatures(&a.client.instanceId, &request)
	if err != nil {
		return localVarReturnValue, err
	}

	localVarReturnValue.TotalCount = int(*response.Body.TotalCount)
	var models []*Model
	for _, modelFeature := range response.Body.ModelFeatures {
		if id, err := strconv.Atoi(*modelFeature.ModelFeatureId); err == nil {
			model := Model{
				ModelId:     id,
				Name:        *modelFeature.Name,
				ProjectName: *modelFeature.ProjectName,
			}
			if id, err := strconv.Atoi(*modelFeature.ProjectId); err == nil {
				model.ProjectId = id
			}

			models = append(models, &model)
		}
	}

	localVarReturnValue.Models = models
	return localVarReturnValue, nil
}

func (a *FsModelApiService) ListModelsByName(pagesize, pagenumber int, projectId, modelName string) (ListModelsResponse, error) {
	var (
		localVarReturnValue ListModelsResponse
	)

	request := paifeaturestore.ListModelFeaturesRequest{}
	request.SetPageSize(int32(pagesize))
	request.SetPageNumber(int32(pagenumber))
	request.SetProjectId(projectId)
	request.SetName(modelName)

	response, err := a.client.ListModelFeatures(&a.client.instanceId, &request)
	if err != nil {
		return localVarReturnValue, err
	}

	localVarReturnValue.TotalCount = int(*response.Body.TotalCount)
	var models []*Model
	for _, modelFeature := range response.Body.ModelFeatures {
		if id, err := strconv.Atoi(*modelFeature.ModelFeatureId); err == nil {
			model := Model{
				ModelId:     id,
				Name:        *modelFeature.Name,
				ProjectName: *modelFeature.ProjectName,
			}
			if id, err := strconv.Atoi(*modelFeature.ProjectId); err == nil {
				model.ProjectId = id
			}

			models = append(models, &model)
		}
	}

	localVarReturnValue.Models = models
	return localVarReturnValue, nil
}
