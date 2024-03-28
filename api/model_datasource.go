package api

import (
	"fmt"

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
}

func (d *Datasource) GenerateDSN(datasourceType string) (DSN string) {
	if datasourceType == constants.Datasource_Type_Hologres {
		if d.TestMode {
			DSN = fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable&connect_timeout=10",
				d.Ak.AccesskeyId, d.Ak.AccesskeySecret, d.PublicAddress, d.Database)
		} else {
			DSN = fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable&connect_timeout=10",
				d.Ak.AccesskeyId, d.Ak.AccesskeySecret, d.VpcAddress, d.Database)
		}
	}
	return
}

func (d *Datasource) NewTableStoreClient() (client *tablestore.TableStoreClient) {
	if d.TestMode {
		client = tablestore.NewClient(d.PublicAddress, d.RdsInstanceId, d.Ak.AccesskeyId, d.Ak.AccesskeySecret)
	} else {
		client = tablestore.NewClient(d.VpcAddress, d.RdsInstanceId, d.Ak.AccesskeyId, d.Ak.AccesskeySecret)
	}
	return
}
