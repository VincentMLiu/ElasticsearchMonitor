package com.act.ElasticsearchMonitor.elasticsearch;

import com.act.ElasticsearchMonitor.elasticsearch.utils.ClusterInfoUtils;
import com.act.ElasticsearchMonitor.elasticsearch.utils.IndicesInfoRequestUtils;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import java.awt.peer.SystemTrayPeer;
import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IndicesCheck {


    private static SimpleDateFormat yyyyMMdd = new SimpleDateFormat("yyyy-MM-dd");

    private static RestClient restClient = RestClient.builder(
            new HttpHost("localhost", 19201, "http"))
            .setMaxRetryTimeoutMillis(10000)
            .build();

    private static int dataNodeNum = 0;


    private static Date today = new Date();

    public static void main(String[] args) throws IOException {


        File newFile = new File("D:\\Desktop\\es集群信息.csv");
        OutputStream os = new FileOutputStream(newFile);
        StringBuffer sb = new StringBuffer();


        //获取所有索引信息
        List<String> indicesInfoList = IndicesInfoRequestUtils.getIndicesList(restClient);
        Map<Pattern, JSONObject> templateMap = IndicesInfoRequestUtils.getTemplateMap(restClient);

        Pattern storeSizePattern = Pattern.compile("(([1-9]\\d*\\.?\\d+)|(0\\.\\d*[1-9])|(\\d+))(mb|kb|b)");

        Pattern dailyIndexPattern = Pattern.compile("\\w+((((19|20)\\d{2})_(0?[13-9]|1[012])_(0?[1-9]|[12]\\d|30))|(((19|20)\\d{2})_(0?[13578]|1[02])_31)|(((19|20)\\d{2})_0?2_(0?[1-9]|1\\d|2[0-8]))|((((19|20)([13579][26]|[2468][048]|0[48]))|(2000))_0?2_29))\\w+");

        dataNodeNum = ClusterInfoUtils.countDataNodesNum(restClient);

        for(String indexInfo: indicesInfoList){
//
            String[] indexInfoSpli = indexInfo.split(",");
            if(indexInfoSpli.length == 10){

                String health = indexInfoSpli[0];
                String status = indexInfoSpli[1];
                String indexName = indexInfoSpli[2];
                String uuid = "'" + indexInfoSpli[3] + "'";
                int priNum = Integer.parseInt(indexInfoSpli[4]);
                int repFactor = Integer.parseInt(indexInfoSpli[5]);
                int docsCount = Integer.parseInt(indexInfoSpli[6]);
                int docsDeleted = Integer.parseInt(indexInfoSpli[7]);
                String storeSize = indexInfoSpli[8];
                System.out.println(indexName);

                if(indexName.equals(".kibana")){
                    continue;
                }
                //1. **模版要求：** 创建索引时需使用`template`，给每一类索引建别名。
                String template = "未建模板";

                for(Pattern pt: templateMap.keySet()){
                    Matcher mt =  pt.matcher(indexName);
                    if(mt.matches()){
                        JSONObject templateJson = templateMap.get(pt);
                        template = templateJson.get("template").toString();
                        break;
                    }
                }
                //2. **分片要求：** （主分片数 ***** 副本份数（`replica factor`）**=** 总分片数 **<=** 集群数据节点（`data node`）总量。数据量少的索引（小于1G或1百万条）主分片数量不大于3，以便数据集中，减少网络传输等耗时。
                String shardsReq = "";
                int shardsNum = priNum*(repFactor+1);
                if( shardsNum > dataNodeNum){
                    shardsReq += "分片数量过多，总分片数(" + shardsNum + ")大于集群数据节点数量 (" + dataNodeNum + ");";
                }
                Matcher smallSizeMatcher = storeSizePattern.matcher(storeSize);
                if((smallSizeMatcher.matches() || docsCount < 1000000) && priNum > 3){
                    shardsReq += "数据量较少，建议未来减少主分片数（" + priNum + "）。";
                }

                //3. **副本要求：**至少创建一份副本`number_of_replicas = 1`，防止集群某节点故障时主分片挂掉变为`red`状态，影响该索引查询。
                String repReq = "";
                if (repFactor == 0){
                    repReq = "未包含副本分片，数据有丢失风险。";
                }

                //4. **数据建模要求：**不同类型的数据使用`index`区分，同一个`index`下不可包含字段不同的`type`类型。
                String strucReq = "";
                //5.a
                String fieldReq = "";
                //5.b
                String _allReq = "";
                //5.c
                String strFieldReq = "";
                //6
                String refreshReq = "";
                //createionDate
                String creationDateReq = "";
                //dailyIndex
                String dailyIndexReq = "";

                JSONObject indexInfoJob = IndicesInfoRequestUtils.getIndexInfo(restClient, indexName);
                if (indexInfoJob!=null){
                    JSONObject indexMappings = indexInfoJob.getJSONObject("mappings");
                    if(indexMappings.entrySet().size() > 1){
                        strucReq = "包含多个type mapping : ";
                        for(String key : indexMappings.keySet()){
                            strucReq += key + "\\n";
                //5. **字段类型要求：**
                //a. 创建索引时需确定好哪些字段是`keyword`，`numeric`等需要倒排索引的 类型，哪些字段是`text`等无需倒排无需分词的类型。禁用`fielddata`。
                            JSONObject type = indexMappings.getJSONObject(key);
                            JSONObject properties = type.getJSONObject("properties");
                            properties.keySet().forEach(
                                    fieldNm -> {
                                        if(fieldNm.equals("")){
                                            System.out.println("aa");
                                        }
                                    }
                            );

                            for(String fieldName : properties.keySet()){
                                JSONObject field = properties.getJSONObject(fieldName);
                                try{
                                    if(field.getString("type").equals("text")){
                                        if(field.containsKey("fielddata") && field.getBoolean("fielddata")){
                                            fieldReq += " [" + fieldName + "] fielddata:true |";
                                        }

                                    }

                                }catch(Exception e){
                                    e.printStackTrace();
                                    continue;
                                }

                //c. 5.0版本之前的`string`类型需替换为`text`或者`keyword`类型。
                                if(field.getString("type").equalsIgnoreCase("string")){
                                    strFieldReq = "包含'string'废弃字段类型";
                                }

                            }
                //b. `_all` 使用要求：无搜索全文的需求，必须禁用`_all`字段。
                            if(!type.containsKey("_all")){
                                _allReq = "未禁用_all字段";
                            }

                        }
                    }

                    JSONObject settings = indexInfoJob.getJSONObject("settings");
                    JSONObject indexSettings = settings.getJSONObject("index");

                //6. **刷新要求：**频繁入库和频繁更新的索引要根据实际情况调整`refresh_interval`参数，至少为 `30s` 以上
                    if(!indexSettings.containsKey("refresh_interval")){
                        refreshReq = "未设置入库刷新频率";
                    }

                //创建时间
                    Date creationDate = new Date(indexSettings.getLong("creation_date"));


                    int dateDifference = (int) ((today.getTime() - creationDate.getTime()) / (24 * 60 * 60 * 1000.0));

                    if(dateDifference >= 90){
                        creationDateReq = "建表日期：" + yyyyMMdd.format(creationDate) +"建表超过90天，请及时清理备份历史数据，关闭或删除无用索引";

                    }

                //天表索引

                    Matcher dailyIndex = dailyIndexPattern.matcher(indexName);
                    if(dailyIndex.matches()){
                        dailyIndexReq = "天表索引建议合并为月表";
                    }

                }



                String lineStr =  health + ","
                        + status + ","
                        + indexName + ","
                        + uuid + ","
                        + priNum + ","
                        + repFactor + ","
                        + docsCount + ","
                        + docsDeleted + ","
                        + storeSize + ","
                        + template + ","
                        + shardsReq + ","
                        + repReq + ","
                        + strucReq + ","
                        + fieldReq + ","
                        + _allReq + ","
                        + strFieldReq + ","
                        + refreshReq + ","
                        + creationDateReq + ","
                        + dailyIndexReq + "\n";




                sb.append(lineStr);

            }else {
                sb.append(indexInfo + "\n");
                System.out.println(indexInfo);
            }






        }



        os.write(sb.toString().getBytes());

        os.flush();
        os.close();
        restClient.close();

//        7. **写磁盘要求：**入库较为频繁的索引，需将`index.translog.durability`改为`async`，调大`index.translog.sync_interval=10s`，避免过多的flush写磁盘操作。


    }






}
