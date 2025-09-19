package domain

import (
	"fmt"
	"strconv"
	"sync"

	"golang.org/x/sync/singleflight"

	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/api"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/constants"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/datasource/featuredb"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/datasource/hologres"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/datasource/igraph"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/datasource/tablestore"
)

type Project struct {
	*api.Project
	OnlineStore      OnlineStore
	FeatureViewMap   sync.Map
	FeatureEntityMap map[string]*FeatureEntity
	ModelMap         sync.Map
	LabelTableMap    sync.Map

	featureViewLoader singleflight.Group
	modelLoader       singleflight.Group
	labelTableLoader  singleflight.Group

	apiClient *api.APIClient
}

func NewProject(p *api.Project, isInitClient bool) *Project {
	project := Project{
		Project:          p,
		FeatureEntityMap: make(map[string]*FeatureEntity),
	}

	switch p.OnlineDatasourceType {
	case constants.Datasource_Type_Hologres:
		onlineStore := &HologresOnlineStore{
			Datasource: p.OnlineDataSource,
		}
		if isInitClient {
			dsn := onlineStore.Datasource.GenerateDSN(constants.Datasource_Type_Hologres)
			useCustomAuth := onlineStore.Datasource.HologresAuth != ""
			hologres.RegisterHologres(onlineStore.Name, dsn, useCustomAuth)
		}
		project.OnlineStore = onlineStore
	case constants.Datasource_Type_IGraph:
		onlineStore := &IGraphOnlineStore{
			Datasource: p.OnlineDataSource,
		}

		if isInitClient {
			if p.OnlineDataSource.TestMode {
				client := igraph.NewGraphClient(p.OnlineDataSource.PublicAddress, p.OnlineDataSource.User, p.OnlineDataSource.Pwd)
				igraph.RegisterGraphClient(onlineStore.Name, client)
			} else {
				client := igraph.NewGraphClient(p.OnlineDataSource.VpcAddress, p.OnlineDataSource.User, p.OnlineDataSource.Pwd)
				igraph.RegisterGraphClient(onlineStore.Name, client)
			}
		}
		project.OnlineStore = onlineStore
	case constants.Datasource_Type_TableStore:
		onlineStore := &TableStoreOnlineStore{
			Datasource: p.OnlineDataSource,
		}

		if isInitClient {
			client := onlineStore.Datasource.NewTableStoreClient()
			tablestore.RegisterTableStoreClient(onlineStore.Name, client)
		}
		project.OnlineStore = onlineStore
	case constants.Datasource_Type_FeatureDB:
		onlineStore := &FeatureDBOnlineStore{
			Datasource: p.OnlineDataSource,
		}

		project.OnlineStore = onlineStore
	default:
		panic("not support onlinestore type")
	}

	if p.FeatureDBAddress != "" && p.FeatureDBToken != "" {
		featuredb.InitFeatureDBClient(p.FeatureDBAddress, p.FeatureDBToken, p.FeatureDBVpcAddress)
	}

	return &project
}

func (p *Project) SetApiClient(apiClient *api.APIClient) {
	p.apiClient = apiClient
}

func (p *Project) GetFeatureView(name string) FeatureView {
	if value, exists := p.FeatureViewMap.Load(name); exists {
		return value.(FeatureView)
	}

	result, err, _ := p.featureViewLoader.Do(name, func() (interface{}, error) {
		if value, exists := p.FeatureViewMap.Load(name); exists {
			return value.(FeatureView), nil
		}
		if err := p.loadFeatureView(name); err != nil {
			return nil, err
		}
		if value, exists := p.FeatureViewMap.Load(name); exists {
			return value.(FeatureView), nil
		}
		return nil, fmt.Errorf("feature view not exist, name=%s", name)
	})
	if err != nil {
		return nil
	}

	return result.(FeatureView)
}

func (p *Project) GetFeatureEntity(name string) *FeatureEntity {
	return p.FeatureEntityMap[name]
}

func (p *Project) GetLabelTable(labelTableId int) *LabelTable {
	if value, exists := p.LabelTableMap.Load(labelTableId); exists {
		return value.(*LabelTable)
	}

	key := strconv.Itoa(labelTableId)
	result, err, _ := p.labelTableLoader.Do(key, func() (interface{}, error) {
		if value, exists := p.LabelTableMap.Load(labelTableId); exists {
			return value.(*LabelTable), nil
		}
		if err := p.loadLabelTable(labelTableId); err != nil {
			return nil, err
		}
		if value, exists := p.LabelTableMap.Load(labelTableId); exists {
			return value.(*LabelTable), nil
		}
		return nil, fmt.Errorf("label table not exist, id=%d", labelTableId)
	})
	if err != nil {
		return nil
	}

	return result.(*LabelTable)
}

func (p *Project) GetModel(name string) *Model {
	if value, exists := p.ModelMap.Load(name); exists {
		return value.(*Model)
	}

	result, err, _ := p.modelLoader.Do(name, func() (interface{}, error) {
		if value, exists := p.ModelMap.Load(name); exists {
			return value.(*Model), nil
		}
		if err := p.loadModelFeature(name); err != nil {
			return nil, err
		}
		if value, exists := p.ModelMap.Load(name); exists {
			return value.(*Model), nil
		}
		return nil, fmt.Errorf("model not exist, name=%s", name)
	})
	if err != nil {
		return nil
	}

	return result.(*Model)
}
func (p *Project) GetModelFeature(name string) *Model {
	if value, exists := p.ModelMap.Load(name); exists {
		return value.(*Model)
	}

	result, err, _ := p.modelLoader.Do(name, func() (interface{}, error) {
		if value, exists := p.ModelMap.Load(name); exists {
			return value.(*Model), nil
		}
		if err := p.loadModelFeature(name); err != nil {
			return nil, err
		}
		if value, exists := p.ModelMap.Load(name); exists {
			return value.(*Model), nil
		}
		return nil, fmt.Errorf("model not exist, name=%s", name)
	})
	if err != nil {
		return nil
	}

	return result.(*Model)
}

func (p *Project) loadFeatureView(featureViewName string) error {
	pageNumber := 1
	pageSize := 100
	for {
		listFeatureViews, err := p.apiClient.FeatureViewApi.ListFeatureViewsByName(int32(pageSize), int32(pageNumber), strconv.Itoa(p.ProjectId), featureViewName)
		if err != nil {
			fmt.Printf("list feature views error, err=%v", err)
			return err
		}
		for _, view := range listFeatureViews.FeatureViews {
			getFeatureViewResponse, err := p.apiClient.FeatureViewApi.GetFeatureViewByID(strconv.Itoa(int(view.FeatureViewId)))
			if err != nil {
				fmt.Printf("get feature view error, err=%v", err)
				return err
			}
			featureView := getFeatureViewResponse.FeatureView
			if featureView.RegisterDatasourceId > 0 {
				getDataSourceResponse, err := p.apiClient.DatasourceApi.DatasourceDatasourceIdGet(featureView.RegisterDatasourceId, 0, "")
				if err != nil {
					fmt.Printf("get datasource error, err=%v", err)
					return err
				}
				featureView.RegisterDataSource = getDataSourceResponse.Datasource
			}

			entity, exist := p.FeatureEntityMap[featureView.FeatureEntityName]
			if !exist {
				fmt.Printf("feature entity not exist, name=%s", featureView.FeatureEntityName)
				return fmt.Errorf("feature entity not exist, name=%s", featureView.FeatureEntityName)
			}
			featureViewDomain := NewFeatureView(featureView, p, entity)
			p.FeatureViewMap.Store(featureView.Name, featureViewDomain)
		}

		if len(listFeatureViews.FeatureViews) == 0 || pageSize*pageNumber > listFeatureViews.TotalCount {
			break
		}

		pageNumber++
	}

	return nil
}

func (p *Project) loadLabelTable(labelTableId int) error {
	getLabelTableResponse, err := p.apiClient.LabelTableApi.GetLabelTableByID(strconv.Itoa(labelTableId))
	if err != nil {
		fmt.Printf("get label table error, err=%v", err)
		return err
	}
	labelTableDomain := NewLabelTable(getLabelTableResponse.LabelTable)
	p.LabelTableMap.Store(labelTableId, labelTableDomain)

	return nil
}

func (p *Project) loadModelFeature(modelFeatureName string) error {
	pageNumber := 1
	pageSize := 100
	for {
		listModelFeatures, err := p.apiClient.FsModelApi.ListModelsByName(pageSize, pageNumber, strconv.Itoa(p.ProjectId), modelFeatureName)
		if err != nil {
			fmt.Printf("list model features error, err=%v", err)
			return err
		}
		for _, m := range listModelFeatures.Models {
			getModelFeatureResponse, err := p.apiClient.FsModelApi.GetModelByID(strconv.Itoa(m.ModelId))
			if err != nil {
				fmt.Printf("get model feature error, err=%v", err)
				return err
			}
			model := getModelFeatureResponse.Model
			labelTableDomain := p.GetLabelTable(model.LabelTableId)
			if labelTableDomain == nil {
				fmt.Printf("label table not exist, id=%d", model.LabelTableId)
				return fmt.Errorf("label table not exist, id=%d", model.LabelTableId)
			}
			modelDomain := NewModel(model, p, labelTableDomain)
			p.ModelMap.Store(model.Name, modelDomain)
		}

		if len(listModelFeatures.Models) == 0 || pageSize*pageNumber > listModelFeatures.TotalCount {
			break
		}

		pageNumber++
	}

	return nil
}
