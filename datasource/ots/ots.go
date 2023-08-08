package ots

import (
	"fmt"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
)

type OTSClient struct {
	client *tablestore.TableStoreClient
}

var (
	otsInstances = make(map[string]*OTSClient)
)

func (o *OTSClient) Init() error {

	return nil
}

func RegisterOTSClient(name string, client *tablestore.TableStoreClient) {
	p := &OTSClient{}
	if _, ok := otsInstances[name]; !ok {
		p.client = client
		otsInstances[name] = p
	}
}

func GetOTSClient(name string) (*OTSClient, error) {
	if _, ok := otsInstances[name]; !ok {
		return nil, fmt.Errorf("OTSClient not found, name:%s", name)
	}

	return otsInstances[name], nil
}

func (o *OTSClient) GetClient() *tablestore.TableStoreClient {
	return o.client
}
