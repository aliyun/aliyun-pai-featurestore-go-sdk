package api

import (
	"strconv"

	paifeaturestore "github.com/alibabacloud-go/paifeaturestore-20230621/v6/client"
)

type LLMConfigApiService service

func (a *LLMConfigApiService) ListLLMConfigsByName(pagesize, pagenumber int, name string) (ListLLMConfigsResponse, error) {
	var localVarReturnValue ListLLMConfigsResponse

	request := paifeaturestore.ListLLMConfigsRequest{}
	request.SetPageSize(int32(pagesize))
	request.SetPageNumber(int32(pagenumber))
	request.SetName(name)

	response, err := a.client.ListLLMConfigs(&a.client.instanceId, &request)
	if err != nil {
		return localVarReturnValue, err
	}

	if response.Body.TotalCount != nil {
		localVarReturnValue.TotalCount = int(*response.Body.TotalCount)
	}
	var configs []*LLMConfig
	for _, item := range response.Body.LLMConfigs {
		if item.LLMConfigId == nil {
			continue
		}
		id, err := strconv.Atoi(*item.LLMConfigId)
		if err != nil {
			continue
		}
		cfg := LLMConfig{LLMConfigId: id}
		if item.Name != nil {
			cfg.Name = *item.Name
		}
		if item.Model != nil {
			cfg.Model = *item.Model
		}
		if item.ModelType != nil {
			cfg.ModelType = *item.ModelType
		}
		if item.WorkspaceId != nil {
			cfg.WorkspaceId = *item.WorkspaceId
		}
		configs = append(configs, &cfg)
	}

	localVarReturnValue.LLMConfigs = configs
	return localVarReturnValue, nil
}
