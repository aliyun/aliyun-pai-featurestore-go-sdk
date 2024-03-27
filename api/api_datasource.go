package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	paifeaturestore "github.com/alibabacloud-go/paifeaturestore-20230621/v3/client"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/constants"
)

type DatasourceApiService service

/*
DatasourceApiService Get datasource By datasource_id
  - @param datasourceId

@return GetDatasourceResponse
*/
func (a *DatasourceApiService) DatasourceDatasourceIdGet(datasourceId int) (GetDatasourceResponse, error) {

	var (
		localVarReturnValue GetDatasourceResponse
	)

	datasourceIdStr := strconv.Itoa(datasourceId)
	response, err := a.client.GetDatasource(&a.client.instanceId, &datasourceIdStr)
	if err != nil {
		return localVarReturnValue, err
	}

	datasource := Datasource{
		DatasourceId: datasourceId,
		Type:         *response.Body.Type,
		Name:         *response.Body.Name,
		Region:       a.client.cfg.regionId,
		WorkspaceId:  *response.Body.WorkspaceId,
	}
	switch *response.Body.Type {
	case "Hologres":
		datasource.Type = constants.Datasource_Type_Hologres
		uris := strings.Split(*response.Body.Uri, "/")
		datasource.Database = uris[1]
		datasource.VpcAddress = fmt.Sprintf("%s-%s-vpc-st.hologres.aliyuncs.com:80", uris[0], a.client.cfg.regionId)
		datasource.PublicAddress = fmt.Sprintf("%s-%s.hologres.aliyuncs.com:80", uris[0], a.client.cfg.regionId)
		//datasource.VpcAddress = fmt.Sprintf("%s-%s.hologres.aliyuncs.com:80", uris[0], a.client.cfg.regionId)
	case "GraphCompute":
		datasource.Type = constants.Datasource_Type_IGraph
		var config map[string]string
		if err := json.Unmarshal([]byte(*response.Body.Config), &config); err == nil {
			datasource.VpcAddress = config["address"]
			datasource.User = config["username"]
			datasource.Pwd = config["password"]
			datasource.PublicAddress = strings.ReplaceAll(config["address"], ".igraph.aliyuncs.com", ".public.igraph.aliyuncs.com")
		}
		datasource.RdsInstanceId = *response.Body.Uri
	case "Tablestore":
		datasource.Type = constants.Datasource_Type_TableStore
		datasource.VpcAddress = fmt.Sprintf("https://%s.%s.vpc.tablestore.aliyuncs.com", *response.Body.Uri, a.client.cfg.regionId)
		datasource.PublicAddress = fmt.Sprintf("https://%s.%s.ots.aliyuncs.com", *response.Body.Uri, a.client.cfg.regionId)
		datasource.RdsInstanceId = *response.Body.Uri
	case "MaxCompute":
		datasource.Type = constants.Datasource_Type_MaxCompute
		datasource.Project = *response.Body.Uri
	case "FeatureDB":
		datasource.Type = constants.Datasource_Type_FeatureDB
		var config map[string]string
		if err := json.Unmarshal([]byte(*response.Body.Config), &config); err == nil {
			datasource.VpcAddress = config["fdb_vpc_address"]
			datasource.PublicAddress = config["fdb_public_address"]
			datasource.Token = config["token"]
		}
	}

	localVarReturnValue.Datasource = &datasource

	return localVarReturnValue, nil
}

func (a *DatasourceApiService) GetFeatureDBDatasourceInfo(isTestMode bool, workspaceId string) (string, string, error) {

	featureDBType := "FeatureDB"
	request := paifeaturestore.ListDatasourcesRequest{
		Type:        &featureDBType,
		WorkspaceId: &workspaceId,
	}
	listDatasourcesResponse, err := a.client.ListDatasources(&a.client.instanceId, &request)
	if err != nil {
		return "", "", err
	}

	for _, datasource := range listDatasourcesResponse.Body.Datasources {
		if _, err := strconv.Atoi(*datasource.DatasourceId); err == nil {
			response, err := a.client.GetDatasource(&a.client.instanceId, datasource.DatasourceId)
			if err != nil {
				return "", "", err
			}
			var config map[string]string
			if err := json.Unmarshal([]byte(*response.Body.Config), &config); err == nil {
				if isTestMode {
					return config["fdb_public_address"], config["token"], nil
				} else {
					return config["fdb_vpc_address"], config["token"], nil
				}
			}
		}
	}

	return "", "", errors.New("FeatureDB datasource not exists")

}
