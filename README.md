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

如果需要从 FeatureStore 在线存储 FeatureDB 中读取数据，初始化 client 需要填写开通 FeatureDB 时设置的用户名和密码（工作空间管理员可在控制台 数据源 页面进行修改）：

```go
username := os.Getenv("FeatureDBUsername")
password := os.Getenv("FeatureDBPassword")
client, err := NewFeatureStoreClient(regionId, accessId, accessKey, projectName, WithFeatureDBLogin(username, password))
```

由于 SDK 是直连 onlinestore 的， client 需要在 VPC 环境运行。 比如 hologres/graphcompute , 需要在指定的 VPC 才能连接。

如果需要在本地非 VPC 环境下进行测试，初始化 client 需要加上 WithTestMode()，此时连接 FeatureStore 和 onlinestore 均会使用公网地址。

```go
client, err := NewFeatureStoreClient(regionId, accessId, accessKey, projectName, WithTestMode())
```

 **注意：** 当使用 WithTestMode() 初始化 client 时，需要确认在线数据源是否已开启公网。通过公网访问onlinestore会有对应数据源的流量开销，可能会产生下行流量费用。因此建议在此模式下只进行测试，生产环境请勿添加 WithTestMode()。

- 获取 (离线/实时) FeatureView 的特征数据

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

- 获取 序列特征 FeatureView 的特征数据
```go
// get project by name
project, err := client.GetProject("holo_p1")
if err != nil {
    // t.Fatal(err)
}

// get featureview by name
seq_feature_view := project.GetFeatureView("seq_fea")
if seq_feature_view == nil {
    // t.Fatal("feature view not exist")
}

// get online features
features, err := seq_feature_view.GetOnlineFeatures([]interface{}{"186569075", "186569870"}, []string{"*"}, nil)
```

[]string{"*"} 代表获取 featureview 中配置的所有在线序列特征名称，也可以指定部分在线序列特征名称。

第三个输入别名（alias）部分对序列特征 featureview 不生效

假设序列特征读取配置为

| 离线序列特征字段 | 事件名称 | 序列长度 | 在线序列特征名称 |
|---------------|---------|---------|---------------|
| click_seq_50_seq | click | 50 | click_seq_50_seq  |
| expr_seq_100_seq | expr | 100 | expr_seq_100 |

返回的数据示例如下

```json
[
  {
    "click_seq_50_seq": "216751275;228787053;220852269;242884721",
    "click_seq_50_seq__event": "click;click;click;click",
    "click_seq_50_seq__event_time": "1699128398;1699128398;1699118623;1699118623",
    "click_seq_50_seq__item_id": "216751275;228787053;220852269;242884721",
    "click_seq_50_seq__playtime": "65.40;72.06;104.69;62.74",
    "click_seq_50_seq__ts": "389018;389018;398793;398793",
    "expr_seq_100": "207474427;216751275;228787053;247136848;270584471;299485479;220852269;242884721;245999124;265863707",
    "expr_seq_100__event": "expr;expr;expr;expr;expr;expr;expr;expr;expr;expr",
    "expr_seq_100__event_time": "1699128398;1699128398;1699128398;1699128398;1699128398;1699128398;1699118623;1699118623;1699118623;1699118623",
    "expr_seq_100__item_id": "207474427;216751275;228787053;247136848;270584471;299485479;220852269;242884721;245999124;265863707",
    "expr_seq_100__playtime": "0.00;0.00;0.00;0.00;0.00;0.00;0.00;0.00;0.00;0.00",
    "expr_seq_100__ts": "389018;389018;389018;389018;389018;389018;398793;398793;398793;398793",
    "user_id": "186569075"
  },
  {
    "click_seq_50_seq": "201741544;236327912;293320498",
    "click_seq_50_seq__event": "click;click;click",
    "click_seq_50_seq__event_time": "1699178245;1699178245;1699178245",
    "click_seq_50_seq__item_id": "201741544;236327912;293320498",
    "click_seq_50_seq__playtime": "97.41;70.32;135.21",
    "click_seq_50_seq__ts": "339171;339171;339171",
    "expr_seq_100": "201741544;224940066;236327912;240253906;247562151;293320498",
    "expr_seq_100__event": "expr;expr;expr;expr;expr;expr",
    "expr_seq_100__event_time": "1699178245;1699178245;1699178245;1699178245;1699178245;1699178245",
    "expr_seq_100__item_id": "201741544;224940066;236327912;240253906;247562151;293320498",
    "expr_seq_100__playtime": "0.00;0.00;0.00;0.00;0.00;0.00",
    "expr_seq_100__ts": "339171;339171;339171;339171;339171;339171",
    "user_id": "186569870"
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

- 获取 ModelFeature 里的特征数据（含序列特征）

注册 ModelFeature 时可以选择 序列特征Feature View里注册的 离线序列特征字段，之后在 FeatureStore Go SDK中便可以获取到对应的 在线序列特征名称。

```go
// get project by name
project, err := client.GetProject("holo_p1")
if err != nil {
    // t.Fatal(err)
}

// get ModelFeature by name
model_feature := project.GetModelFeature("rank_v1")
if model_feature == nil {
    // t.Fatal("model feature not exist")
}

// get online features
features, err := model_feature.GetOnlineFeatures(map[string][]interface{}{"user_id": {"186569075", "186569870"}, "item_id":{"299485479", "207474427"}} )
```

序列特征对应 FeatureEntity 一般为 user, 示例中有两个 join_id, user_id 和 item_id。获取特征的时候需要设置相同的id数量。

假设ModelFeature注册时选择的序列特征字段为：
假设序列特征读取配置为

| 离线序列特征字段 | 事件名称 | 序列长度 | 在线序列特征名称 |
|---------------|---------|---------|---------------|
| click_seq_50_seq | click | 50 | click_seq_50_seq  |

返回数据示例

```json
[
  {
    "age": 51,
    "author": 147848300,
    "category": "7",
    "city": "",
    "click_count": 0,
    "click_seq_50_seq": "216751275;228787053;220852269;242884721",
    "click_seq_50_seq__event": "click;click;click;click",
    "click_seq_50_seq__event_time": "1699128398;1699128398;1699118623;1699118623",
    "click_seq_50_seq__item_id": "216751275;228787053;220852269;242884721",
    "click_seq_50_seq__playtime": "65.40;72.06;104.69;62.74",
    "click_seq_50_seq__ts": "391447;391447;401222;401222",
    "duration": 48,
    "follow_cnt": 2,
    "follower_cnt": 0,
    "gender": "female",
    "item_cnt": 0,
    "item_id": 299485479,
    "praise_count": 2,
    "pub_time": 1697885713,
    "register_time": 1696582012,
    "tags": "0",
    "title": "#健身打卡",
    "user_id": "186569075"
  },
  {
    "age": 28,
    "author": 119734983,
    "category": "18",
    "city": "",
    "click_count": 0,
    "click_seq_50_seq": "201741544;236327912;293320498",
    "click_seq_50_seq__event": "click;click;click",
    "click_seq_50_seq__event_time": "1699178245;1699178245;1699178245",
    "click_seq_50_seq__item_id": "201741544;236327912;293320498",
    "click_seq_50_seq__playtime": "97.41;70.32;135.21",
    "click_seq_50_seq__ts": "341600;341600;341600",
    "duration": 15,
    "follow_cnt": 0,
    "follower_cnt": 2,
    "gender": "male",
    "item_cnt": 0,
    "item_id": 207474427,
    "praise_count": 79,
    "pub_time": 1697731285,
    "register_time": 1699135393,
    "tags": "1",
    "title": "#成语故事",
    "user_id": "186569870"
  }
]
```

也可以指定某个 FeatureEntity，比如指定 FeatureEntity 为 user，则会把 ModelFeature 下的 user(FeatureEntity) 对应的特征（包括序列特征）全部获取到。

```go
features, err := model_feature.GetOnlineFeaturesWithEntity(map[string][]interface{}{"user_id": {"186569075", "186569870"}}, "user")
```

返回数据示例

```json
[
  {
    "age": 51,
    "city": "",
    "click_seq_50_seq": "216751275;228787053;220852269;242884721",
    "click_seq_50_seq__event": "click;click;click;click",
    "click_seq_50_seq__event_time": "1699128398;1699128398;1699118623;1699118623",
    "click_seq_50_seq__item_id": "216751275;228787053;220852269;242884721",
    "click_seq_50_seq__playtime": "65.40;72.06;104.69;62.74",
    "click_seq_50_seq__ts": "392212;392212;401987;401987",
    "follow_cnt": 2,
    "follower_cnt": 0,
    "gender": "female",
    "item_cnt": 0,
    "register_time": 1696582012,
    "tags": "0",
    "user_id": "186569075"
  },
  {
    "age": 28,
    "city": "",
    "click_seq_50_seq": "201741544;236327912;293320498",
    "click_seq_50_seq__event": "click;click;click",
    "click_seq_50_seq__event_time": "1699178245;1699178245;1699178245",
    "click_seq_50_seq__item_id": "201741544;236327912;293320498",
    "click_seq_50_seq__playtime": "97.41;70.32;135.21",
    "click_seq_50_seq__ts": "342365;342365;342365",
    "follow_cnt": 0,
    "follower_cnt": 2,
    "gender": "male",
    "item_cnt": 0,
    "register_time": 1699135393,
    "tags": "1",
    "user_id": "186569870"
  }
]
```

