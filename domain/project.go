package domain

import (
	"strconv"

	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/datasource/hologres"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/datasource/igraph"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/datasource/mysqldb"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/datasource/redisdb"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/swagger"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/swagger/common"
)

type Project struct {
	*swagger.Project
	OnlineStore      OnlineStore
	FeatureViewMap   map[string]*FeatureView
	FeatureEntityMap map[string]*FeatureEntity
	ModelMap         map[string]*Model
}

func NewProject(p *swagger.Project) *Project {
	project := Project{
		Project:          p,
		FeatureViewMap:   make(map[string]*FeatureView),
		FeatureEntityMap: make(map[string]*FeatureEntity),
		ModelMap:         make(map[string]*Model),
	}

	switch p.OnlineDatasourceType {
	case common.Datasource_Type_Hologres:
		onlineStore := &HologresOnlineStore{
			Datasource: p.OnlineDataSource,
		}
		dsn := onlineStore.Datasource.GenerateDSN(common.Datasource_Type_Hologres)
		hologres.RegisterHologres(onlineStore.Name, dsn)
		project.OnlineStore = onlineStore
	case common.Datasource_Type_Mysql:
		onlineStore := &MysqlOnlineStore{
			Datasource: p.OnlineDataSource,
		}
		dsn := onlineStore.Datasource.GenerateDSN(common.Datasource_Type_Mysql)
		mysqldb.RegisterMysql(onlineStore.Name, dsn)
		project.OnlineStore = onlineStore
	case common.Datasource_Type_IGraph:
		onlineStore := &IGraphOnlineStore{
			Datasource: p.OnlineDataSource,
		}

		client := igraph.NewGraphClient(p.OnlineDataSource.VpcAddress, p.OnlineDataSource.MysqlUser, p.OnlineDataSource.MysqlPwd)
		igraph.RegisterGraphClient(onlineStore.Name, client)
		project.OnlineStore = onlineStore
	case common.Datasource_Type_Redis:
		onlineStore := &RedisOnlineStore{
			Datasource: p.OnlineDataSource,
		}

		db := 0
		if dbv, err := strconv.Atoi(p.OnlineDataSource.Database); err == nil {
			db = dbv
		}
		redisdb.RegisterRedis(onlineStore.Name, p.OnlineDataSource.VpcAddress, p.OnlineDataSource.Pwd, db)
		project.OnlineStore = onlineStore

	default:
		panic("not support onlinestore type")
	}

	return &project
}

func (p *Project) GetFeatureView(name string) *FeatureView {
	return p.FeatureViewMap[name]
}

func (p *Project) GetFeatureEntity(name string) *FeatureEntity {
	return p.FeatureEntityMap[name]
}

func (p *Project) GetModel(name string) *Model {
	return p.ModelMap[name]
}
