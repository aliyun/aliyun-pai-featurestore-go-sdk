package fdbserverpb

import (
	"bytes"
	"fmt"
	"io"
	"net/http"

	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/datasource/featuredb"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/domain"
	"google.golang.org/protobuf/proto"
)

func BatchWriteBloomKV(project *domain.Project, featureView domain.FeatureView, request *BatchWriteKVReqeust) error {
	fdbClient, err := featuredb.GetFeatureDBClient()
	if err != nil {
		return err
	}

	requestData, err := proto.Marshal(request)
	if err != nil {
		return err
	}
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/api/v1/tables/%s/%s/%s/bloom_write",
		fdbClient.Address, project.InstanceId, project.ProjectName, featureView.GetName()), bytes.NewReader(requestData))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fdbClient.Token)
	req.Header.Set("Auth", project.Signature)

	response, err := fdbClient.Client.Do(req)
	if err != nil {
		return err
	}

	defer response.Body.Close()

	responseData, err := io.ReadAll(response.Body)
	if err != nil {
		return err
	}

	if response.StatusCode != 200 {
		return fmt.Errorf("status code: %d, response body: %s", response.StatusCode, string(responseData))
	}

	responseBody := &BatchWriteKVResponse{}
	if err := proto.Unmarshal(responseData, responseBody); err != nil {
		return err
	}

	if len(responseBody.ErrorMessages) > 0 {
		return fmt.Errorf("error messages: %s", responseBody.ErrorMessages)
	}

	return nil
}

func TestBloomItems(project *domain.Project, featureView domain.FeatureView, request *TestBloomItemsRequest) ([]bool, error) {
	fdbClient, err := featuredb.GetFeatureDBClient()
	if err != nil {
		return nil, err
	}

	requestData, err := proto.Marshal(request)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/api/v1/tables/%s/%s/%s/test_bloom_items",
		fdbClient.Address, project.InstanceId, project.ProjectName, featureView.GetName()), bytes.NewReader(requestData))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fdbClient.Token)
	req.Header.Set("Auth", project.Signature)

	response, err := fdbClient.Client.Do(req)
	if err != nil {
		return nil, err
	}

	defer response.Body.Close()

	responseData, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	if response.StatusCode != 200 {
		return nil, fmt.Errorf("status code: %d, response body: %s", response.StatusCode, string(responseData))
	}
	responseBody := &TestBloomItemsResponse{}
	if err := proto.Unmarshal(responseData, responseBody); err != nil {
		return nil, err
	}

	return responseBody.Tests, nil

}
