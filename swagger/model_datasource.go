package swagger

import (
	"fmt"

	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/swagger/common"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
)

type Datasource struct {
	DatasourceId  int32  `json:"datasource_id,omitempty"`
	AkId          int32  `json:"ak_id,omitempty"`
	Type_         string `json:"type"`
	Name          string `json:"name"`
	Region        string `json:"region,omitempty"`
	VpcAddress    string `json:"vpc_address,omitempty"`
	Project       string `json:"project,omitempty"`
	Database      string `json:"database,omitempty"`
	Token         string `json:"token,omitempty"`
	MysqlUser     string `json:"mysql_user,omitempty"`
	MysqlPwd      string `json:"mysql_pwd,omitempty"`
	Pwd           string `json:"pwd,omitempty"`
	RdsInstanceId string `json:"rds_instance_id,omitempty"`

	Ak Ak `json:"-"`
}

func (d *Datasource) GenerateDSN(datasourceType string) (DSN string) {
	if datasourceType == common.Datasource_Type_Mysql {
		DSN = fmt.Sprintf("%s:%s@tcp(%s)/%s", d.MysqlUser, d.MysqlPwd, d.VpcAddress, d.Database)
	}
	if datasourceType == common.Datasource_Type_Hologres {
		DSN = fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable&connect_timeout=1",
			d.Ak.AccesskeyId, d.Ak.AccesskeySecret, d.VpcAddress, d.Database)
	}
	return
}

func (d *Datasource) NewOTSClient() (client *tablestore.TableStoreClient) {
	client = tablestore.NewClient(d.VpcAddress, d.RdsInstanceId, d.Ak.AccesskeyId, d.Ak.AccesskeySecret)
	return
}
