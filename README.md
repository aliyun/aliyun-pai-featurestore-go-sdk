# aliyun-pai-featurestore-go-sdk
阿里云 PAI 平台 PAI-FeatureStore Go Sdk

# 安装 

```
go get github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2
```

# 使用方式

- 初始化 client

```go
accessId := os.Getenv("AccessId")
accessKey := os.Getenv("AccessKey")
regionId := "cn-hangzhou"
projectName := "fs_test_ots"

client, err := NewFeatureStoreClient(regionId, accessId, accessKey, projectName)
```

由于 SDK 是直连 onlinestore 的， client 需要在 VPC 环境运行。 比如 hologres/graphcompute , 需要在指定的 VPC 才能连接。

- 获取 FeatureView 的特征数据

```go
// get project by name
project, err := client.GetProject("fs_test_ots")
if err != nil {
    // t.Fatal(err)
}

// get featureview by name
user_feature_view := project.GetFeatureView("user_fea")
if user_feature_view == nil {
    // t.Fatal("feature view not exist")
}

// get online features
features, err := user_feature_view.GetOnlineFeatures([]interface{}{"100043186", "100060369"}, []string{"*"}, nil)
```

[]string{"*"} 代表获取 featureview 下的所有特征， 也可以指定部分特征名称。

返回的数据示例如下

```json
[
    {
        "city":"合肥市",
        "follow_cnt":1,
        "gender":"male",
        "user_id":"100043186"
    },
    {
        "city":"",
        "follow_cnt":5,
        "gender":"male",
        "user_id":"100060369"
    }
]
```

- 获取 ModelFeature 里的特征数据

```golang
// get project by name
project, err := client.GetProject("fs_test_ots")
if err != nil {
    // t.Fatal(err)
}

// get ModelFeature by name
model_feature := project.GetModelFeature("rank")
if model_feature == nil {
    // t.Fatal("model feature not exist")
}

// get online features
features, err := model_feature.GetOnlineFeatures(map[string][]interface{}{"user_id": {"100000676", "100004208"}, "item_id":{"238038872", "264025480"}} )
```

ModelFeature 可以关联多个 FeatureEntity,  可以设置多个 join_id, 然后特征统一返回。

示例中有两个 join_id, user_id 和 item_id 。 获取特征的时候需要设置相同的 id 数量

返回数据示例

```json
[
    {
        "age":26,
        "author":100015828,
        "category":"14",
        "city":"沈阳市",
        "duration":63,
        "gender":"male",
        "item_id":"238038872",
        "user_id":"100000676"
    },
    {
        "age":23,
        "author":100015828,
        "category":"15",
        "city":"西安市",
        "duration":22,
        "gender":"male",
        "item_id":"264025480",
        "user_id":"100004208"
    }
]
```

也可以指定某个 FeatureEntity, 把 FeatureEntity 对应的特征一块返回。

```go
// user 是 FeatureEntity 名称
features, err := model_feature.GetOnlineFeaturesWithEntity(map[string][]interface{}{"user_id": {"100000676", "100004208"}}, "user" )
```

上面的含义是把  ModelFeature 下的 user(FeatureEntity) 对应的特征全部获取到。

返回数据示例

```json
[
    {
        "age":26,
        "city":"沈阳市",
        "gender":"male",
        "user_id":"100000676"
    },
    {
        "age":23,
        "city":"西安市",
        "gender":"male",
        "user_id":"100004208"
    }
]
```

