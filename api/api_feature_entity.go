package api

import (
	"strconv"

	paifeaturestore "github.com/alibabacloud-go/paifeaturestore-20230621/v6/client"
)

type FeatureEntityApiService service

/*
FeatureEntityApiService List FeatureEntities
  - @param ctx context.Context - for authentication, logging, cancellation, deadlines, tracing, etc. Passed from http.Request or context.Background().

@return InlineResponse20079
*/
func (a *FeatureEntityApiService) ListFeatureEntities(pagesize, pagenumber int32, projectId string) (ListFeatureEntitiesResponse, error) {
	var (
		localVarReturnValue ListFeatureEntitiesResponse
	)
	request := paifeaturestore.ListFeatureEntitiesRequest{}
	request.SetProjectId(projectId)
	request.SetPageSize(pagesize)
	request.SetPageNumber(pagenumber)

	response, err := a.client.ListFeatureEntities(&a.client.instanceId, &request)
	if err != nil {
		return localVarReturnValue, err
	}

	localVarReturnValue.TotalCount = int(*response.Body.TotalCount)
	var featureEntities []*FeatureEntity

	for _, entity := range response.Body.FeatureEntities {
		id, err := strconv.Atoi(*entity.FeatureEntityId)
		if err == nil {
			featureEntity := FeatureEntity{
				FeatureEntityId:     id,
				FeatureEntityName:   *entity.Name,
				FeatureEntityJoinid: *entity.JoinId,
				ProjectName:         *entity.ProjectName,
			}
			if entity.ParentJoinId != nil {
				featureEntity.ParentJoinId = *entity.ParentJoinId
			}
			if id, err := strconv.Atoi(*entity.ProjectId); err == nil {
				featureEntity.ProjectId = id
			}
			if entity.ParentFeatureEntityId != nil {
				if id, err := strconv.Atoi(*entity.ParentFeatureEntityId); err == nil {
					featureEntity.ParentFeatureEntityId = id
				}
			}

			featureEntities = append(featureEntities, &featureEntity)
		}
	}

	localVarReturnValue.FeatureEntities = featureEntities

	return localVarReturnValue, nil
}
