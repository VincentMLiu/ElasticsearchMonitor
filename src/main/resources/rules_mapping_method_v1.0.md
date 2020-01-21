# ES集群使用规范_v1.0



## 创建索引规范


* getIndicesList获取所有索引

1. **模版要求：** 创建索引时需使用`template`，给每一类索引建别名。

2. **分片要求：** （主分片数 ***** 副本份数（`replica factor`）**=** 总分片数 **<=** 集群数据节点（`data node`）总量。数据量少的索引（小于1G或1百万条）主分片数量不大于3，以便数据集中，减少网络传输等耗时。

3. **副本要求：**至少创建一份副本`number_of_replicas = 1`，防止集群某节点故障时主分片挂掉变为`red`状态，影响该索引查询。

4. **数据建模要求：**不同类型的数据使用`index`区分，同一个`index`下不可包含字段不同的`type`类型。

5. **字段类型要求：**



    a. 创建索引时需确定好哪些字段是`keyword`，`numeric`等需要倒排索引的 类型，哪些字段是`text`等无需倒排无需分词的类型。禁用`fielddata`。



    b. `_all` 使用要求：无搜索全文的需求，必须禁用`_all`字段。



    c. 5.0版本之前的`string`类型需替换为`text`或者`keyword`类型。



6. **刷新要求：**频繁入库和频繁更新的索引要根据实际情况调整`refresh_interval`参数，至少为 `30s` 以上。

7. **写磁盘要求：**入库较为频繁的索引，需将`index.translog.durability`改为`async`，调大`index.translog.sync_interval=10s`，避免过多的flush写磁盘操作。

```



索引模板示例：

    

    PUT /_template/t_test_template?pretty

    {

    	"template": "t_test_template*", //匹配的模板

    	"settings" : {

    		"index" : {

    			"number_of_shards" : 3, //主分片数

    			"number_of_replicas" : 1, //副本份数

    			"refresh_interval" : "30s", //刷新translog的频率

    			"translog.durability" : "async", //translog写盘方式为异步

    			"translog.sync_interval" : "10s" //每10秒从内存flush到磁盘一次

    		}

    	},

    	"aliases" : {

            "t_test_template" : {}

        },

    	"mappings": {

    	"t_test_template": {

    	  "_all":       { "enabled": false  }, //_all字段禁用

    	  "properties": {

    					"type":    { "type": "integer"},

    					"isFraud":    { "type": "boolean"},

    					"modifyTime":    { "type": "date", "format": "yyyyMMddHHmmss||yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"},

    					"msg_content":    { "type": "text" , "index" : false, "fielddata": false},

    					"send_id":    { "type": "keyword"}

          }

        }

      }

    }
    
```



## 入库规范



1. **接口使用要求：**需使用`rest`接口，禁用`tcp`接口。

2. 必须使用批量入库，`bulk`API。

3. 每次`bulk`请求不超过10000条。

4. 避免单条数据过大，不要超过100M。

5. 入库时建议根据某个平均分布的离散值进行`routing`。（例如：天）


```
入库示例：

    

    POST _bulk?pretty

    { "index" : { "_index" : "t_test_template123", "_type" : "t_test_template", "_id" : "1" , "routing" : "20191225"} }

    { "type" : 1, "send_id": "123" }

    { "index" : { "_index" : "t_test_template456", "_type" : "t_test_template", "_id" : "1" , "routing" : "20191225"} }

    { "type" : 1, "send_id": "456" }

    { "index" : { "_index" : "t_test_template456", "_type" : "t_test_template", "_id" : "2" , "routing" : "20191225"} }

    { "type" : 1, "send_id": "456" }

    { "index" : { "_index" : "t_test_template456", "_type" : "t_test_template", "_id" : "3" , "routing" : "20191225"} }

    { "type" : 1, "send_id": "456" }

    { "index" : { "_index" : "t_test_template456", "_type" : "t_test_template", "_id" : "4" , "routing" : "20191225"} }

    { "type" : 1, "send_id": "456" }

```

## 搜索规范



1. **接口使用要求：**需使用`rest`接口，禁用`tcp`接口。

2. 禁用`_all`字段搜索。

3. **禁止**不带索引名或别名，别名内包含的`index`数目不得超过10，天表查询可根据实际情况进行按月合并。

4. 避免大结果集，查询`size`**禁止**超过10000。遍历数据确保使用`scroll`API。

5. 翻页请求 `from + size` 之和不能超过10000。

6. 搜索时可对`routing`参数加以利用。（例如：天）


```

搜索示例：

    

    GET t_test_template/_search?pretty

    {

    	"from" : 10

    	"size" : 100,

      "query": {

        "terms": {

          "_routing": [ "20191225" ] //按天routing

        }

      }

    }
    
```



## 索引维护建议



1. 设计索引模板前，提前规划好每日数据接入量，数据留存量，以及留存周期。定期关闭或删除不需要再搜索的索引。

2. 将历史数据同步到hive或hdfs中做备份，节省ES的内存空间。需要时可将数据导回ES内进行搜索。

3. 为了便于搜索和数据归并，天表数据可定期使用`reindex` API同步至月表。同时关闭天表。


```

    POST _reindex

    {

      "source": {

        "index": "t_test_template20191225"

      },

      "dest": {

        "index": "t_test_template201912"

      }

    }
```