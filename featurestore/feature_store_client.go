package featurestore

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/antihax/optional"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/domain"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/swagger"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/swagger/common"
)

type ClientOption func(c *FeatureStoreClient)

func WithLogger(l Logger) ClientOption {
	return func(e *FeatureStoreClient) {
		e.Logger = l
	}
}

func WithErrorLogger(l Logger) ClientOption {
	return func(e *FeatureStoreClient) {
		e.ErrorLogger = l
	}
}
func WithToken(token string) ClientOption {
	return func(e *FeatureStoreClient) {
		e.Token = token
	}
}

type FeatureStoreClient struct {
	// Host FeatureStore server host
	Host string
	// Token the request header Authorization
	Token string

	APIClient *swagger.APIClient

	projectMap map[string]*domain.Project

	// Logger specifies a logger used to report internal changes within the writer
	Logger Logger

	// ErrorLogger is the logger to report errors
	ErrorLogger Logger
}

func NewFeatureStoreClient(host string, opts ...ClientOption) (*FeatureStoreClient, error) {
	client := FeatureStoreClient{
		Host:       host,
		projectMap: make(map[string]*domain.Project, 0),
	}

	for _, opt := range opts {
		opt(&client)
	}

	if err := client.Validate(); err != nil {
		return nil, err
	}

	cfg := swagger.NewConfiguration(client.Host, client.Token)
	client.APIClient = swagger.NewAPIClient(cfg)

	client.LoadProjectData()

	go client.loopLoadProjectData()

	return &client, nil
}

// Validate check the  FeatureStoreClient value
func (e *FeatureStoreClient) Validate() error {
	if e.Host == "" {
		return errors.New("host is empty")
	}

	return nil
}

func (c *FeatureStoreClient) GetProject(name string) (*domain.Project, error) {
	project, ok := c.projectMap[name]
	if ok {
		return project, nil
	}

	return nil, fmt.Errorf("not found project, name:%s", name)
}

func (c *FeatureStoreClient) logError(err error) {
	if c.ErrorLogger != nil {
		c.ErrorLogger.Printf(err.Error())
		return
	}

	if c.Logger != nil {
		c.Logger.Printf(err.Error())
	}
}

// LoadProjectData specifies a function to load data from featurestore server
func (c *FeatureStoreClient) LoadProjectData() {
	projectData := make(map[string]*domain.Project, 0)

	listProjectsResponse, err := c.APIClient.FsProjectApi.ListProjects(context.Background())
	if err != nil {
		c.logError(fmt.Errorf("list projects error, err=%v", err))
		return
	}

	if listProjectsResponse.Code != common.CODE_OK {
		c.logError(fmt.Errorf("list projects error, requestid=%s,code=%s, msg=%s", listProjectsResponse.RequestId, listProjectsResponse.Code, listProjectsResponse.Message))
		return
	}
	for _, p := range listProjectsResponse.Data["projects"] {
		// get datasource
		getDataSourceResponse, err := c.APIClient.DatasourceApi.DatasourceDatasourceIdGet(context.Background(), p.OnlineDatasourceId)
		if err != nil {
			c.logError(fmt.Errorf("get datasource error, err=%v", err))
			continue
		}

		if getDataSourceResponse.Code != common.CODE_OK {
			c.logError(fmt.Errorf("get datasource error, requestid=%s,code=%s, msg=%s", getDataSourceResponse.RequestId, getDataSourceResponse.Code, getDataSourceResponse.Message))
			continue
		}

		p.OnlineDataSource = getDataSourceResponse.Data["datasource"]
		if p.OnlineDataSource.AkId > 0 {
			// get ak
			getAkResponse, err := c.APIClient.AkApi.AkAkIdGet(context.Background(), int64(p.OnlineDataSource.AkId))
			if err == nil && getAkResponse.Code == common.CODE_OK {
				p.OnlineDataSource.Ak = getAkResponse.Data["ak"]
			}
		}

		project := domain.NewProject(p)
		projectData[project.ProjectName] = project

		// get feature entities
		listFeatureEntitiesResponse, err := c.APIClient.FeatureEntityApi.ListFeatureEntities(context.Background())
		if err != nil {
			c.logError(fmt.Errorf("list feature entities error, err=%v", err))
			continue
		}
		if listFeatureEntitiesResponse.Code != common.CODE_OK {
			c.logError(fmt.Errorf("list feature entities error, requestid=%s,code=%s, msg=%s", listFeatureEntitiesResponse.RequestId, listFeatureEntitiesResponse.Code, listFeatureEntitiesResponse.Message))
			continue
		}

		for _, entity := range listFeatureEntitiesResponse.Data["feature_entities"] {
			if entity.ProjectId == project.ProjectId {
				project.FeatureEntityMap[entity.FeatureEntityName] = domain.NewFeatureEntity(entity)
			}
		}

		var (
			pagesize   = 100
			pagenumber = 1
		)
		// get feature views
		for {
			listFeatureViews, err := c.APIClient.FeatureViewApi.ListFeatureViews(context.Background(), &swagger.FeatureViewApiListFeatureViewsOpts{Pagesize: optional.NewInt32(int32(pagesize)),
				Pagenumber: optional.NewInt32(int32(pagenumber)), ProjectId: optional.NewInt32(int32(project.ProjectId))})
			if err != nil {
				c.logError(fmt.Errorf("list feature views error, err=%v", err))
				continue
			}
			if listFeatureViews.Code != common.CODE_OK {
				c.logError(fmt.Errorf("list feature views error, requestid=%s,code=%s, msg=%s", listFeatureViews.RequestId, listFeatureViews.Code, listFeatureViews.Message))
				continue
			}

			for _, view := range listFeatureViews.Data.FeatureViews {
				getFeatureViewResponse, err := c.APIClient.FeatureViewApi.GetFeatureViewByID(context.Background(), strconv.Itoa(int(view.FeatureViewId)))
				if err != nil {
					c.logError(fmt.Errorf("get feature view error, err=%v", err))
					continue
				}
				if getFeatureViewResponse.Code != common.CODE_OK {
					c.logError(fmt.Errorf("get feature view error, requestid=%s,code=%s, msg=%s", getFeatureViewResponse.RequestId, getFeatureViewResponse.Code, getFeatureViewResponse.Message))
					continue
				}
				if len(getFeatureViewResponse.Data["feature_views"]) == 1 {
					featureView := getFeatureViewResponse.Data["feature_views"][0]
					featureViewDomain := domain.NewFeatureView(&featureView, project, project.FeatureEntityMap[featureView.FeatureEntityName])
					project.FeatureViewMap[featureView.Name] = featureViewDomain
				}

			}

			if len(listFeatureViews.Data.FeatureViews) == 0 || pagesize*pagenumber > listFeatureViews.Data.TotalCount {
				break
			}

			pagenumber++

		}

		pagenumber = 1
		// get model
		for {
			listModelsResponse, err := c.APIClient.FsModelApi.ListModels(context.Background(), &swagger.FsModelApiListModelsOpts{Pagesize: optional.NewInt32(int32(pagesize)),
				Pagenumber: optional.NewInt32(int32(pagenumber)), ProjectId: optional.NewInt32(int32(project.ProjectId))})
			if err != nil {
				c.logError(fmt.Errorf("list models error, err=%v", err))
				continue
			}
			if listModelsResponse.Code != common.CODE_OK {
				c.logError(fmt.Errorf("list models error, requestid=%s,code=%s, msg=%s", listModelsResponse.RequestId, listModelsResponse.Code, listModelsResponse.Message))
				continue
			}

			for _, m := range listModelsResponse.Data.Models {
				getModelResponse, err := c.APIClient.FsModelApi.GetModelByID(context.Background(), strconv.Itoa(m.ModelId))
				if err != nil {
					c.logError(fmt.Errorf("get model error, err=%v", err))
					continue
				}
				if getModelResponse.Code != common.CODE_OK {
					c.logError(fmt.Errorf("get model error, requestid=%s,code=%s, msg=%s", getModelResponse.RequestId, getModelResponse.Code, getModelResponse.Message))
					continue
				}
				if len(getModelResponse.Data.Models) == 1 {
					model := getModelResponse.Data.Models[0]
					modelDomain := domain.NewModel(&model, project)
					project.ModelMap[model.Name] = modelDomain
				}

			}

			if len(listModelsResponse.Data.Models) == 0 || pagenumber*pagesize > int(listModelsResponse.Data.TotalCount) {
				break
			}

			pagenumber++

		}

	}

	if len(projectData) > 0 {
		c.projectMap = projectData
	}
}

func (c *FeatureStoreClient) loopLoadProjectData() {
	for {
		time.Sleep(time.Minute)
		c.LoadProjectData()
	}
}
