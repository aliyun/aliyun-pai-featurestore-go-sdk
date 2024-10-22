package api

import (
	"fmt"
	"net/url"

	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/constants"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
)

type Datasource struct {
	DatasourceId  int    `json:"datasource_id,omitempty"`
	Type          string `json:"type"`
	Name          string `json:"name"`
	Region        string `json:"region,omitempty"`
	WorkspaceId   string `json:"workspace_id"`
	VpcAddress    string `json:"vpc_address,omitempty"`
	PublicAddress string `json:"public_address,omitempty"`
	Project       string `json:"project,omitempty"`
	Database      string `json:"database,omitempty"`
	Token         string `json:"token,omitempty"`
	Pwd           string `json:"pwd,omitempty"`
	User          string `json:"user,omitempty"`
	RdsInstanceId string `json:"rds_instance_id,omitempty"`

	Ak Ak `json:"-"`

	TestMode bool `json:"-"`

	HologresPrefix string `json:"-"`
}

func (d *Datasource) GenerateDSN(datasourceType string) (DSN string) {
	if datasourceType == constants.Datasource_Type_Hologres {
		if d.TestMode {
			if d.Ak.SecurityToken != "" {
				DSN = fmt.Sprintf("postgres://%s%s:%s@%s/%s?sslmode=disable&connect_timeout=10&options=sts_token=%s", d.HologresPrefix,
					d.Ak.AccesskeyId, d.Ak.AccesskeySecret, d.PublicAddress, d.Database, url.QueryEscape(d.Ak.SecurityToken))
			} else {
				DSN = fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable&connect_timeout=10",
					d.Ak.AccesskeyId, d.Ak.AccesskeySecret, d.PublicAddress, d.Database)
			}
		} else {
			if d.Ak.SecurityToken != "" {
				DSN = fmt.Sprintf("postgres://%s%s:%s@%s/%s?sslmode=disable&connect_timeout=10&options=sts_token=%s", d.HologresPrefix,
					d.Ak.AccesskeyId, d.Ak.AccesskeySecret, d.VpcAddress, d.Database, url.QueryEscape(d.Ak.SecurityToken))
			} else {
				DSN = fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable&connect_timeout=10",
					d.Ak.AccesskeyId, d.Ak.AccesskeySecret, d.VpcAddress, d.Database)
			}
		}
	}
	return
}

func (d *Datasource) NewTableStoreClient() (client *tablestore.TableStoreClient) {
	if d.TestMode {
		if d.Ak.SecurityToken != "" {
			client = tablestore.NewClientWithConfig(d.PublicAddress, d.RdsInstanceId, d.Ak.AccesskeyId, d.Ak.AccesskeySecret, d.Ak.SecurityToken, nil)
		} else {
			client = tablestore.NewClient(d.PublicAddress, d.RdsInstanceId, d.Ak.AccesskeyId, d.Ak.AccesskeySecret)
		}
	} else {
		if d.Ak.SecurityToken != "" {
			client = tablestore.NewClientWithConfig(d.VpcAddress, d.RdsInstanceId, d.Ak.AccesskeyId, d.Ak.AccesskeySecret, d.Ak.SecurityToken, nil)
		} else {
			client = tablestore.NewClient(d.VpcAddress, d.RdsInstanceId, d.Ak.AccesskeyId, d.Ak.AccesskeySecret)
		}
	}
	return
}
