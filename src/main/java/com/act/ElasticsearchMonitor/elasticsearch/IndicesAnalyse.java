package com.act.ElasticsearchMonitor.elasticsearch;

import com.act.ElasticsearchMonitor.elasticsearch.utils.ClusterInfoUtils;
import com.act.ElasticsearchMonitor.elasticsearch.utils.ConfigerationUtils;
import com.act.ElasticsearchMonitor.elasticsearch.utils.Csv2Xlsx;
import com.act.ElasticsearchMonitor.elasticsearch.utils.IndicesInfoRequestUtils;
import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IndicesAnalyse {


    private static SimpleDateFormat yyyyMMdd = new SimpleDateFormat("yyyy-MM-dd");

    private static RestClient restClient;

    private static int dataNodeNum = 0;


    private static Date today = new Date();

    public static void main(String[] args) throws IOException {

        String resultFileName = "indicesAnalyse_" + yyyyMMdd.format(new Date()) +".csv";


        //ES连接信息
        ConfigerationUtils.init("indicesAnalyse.properties");
        String esHost = ConfigerationUtils.get("esHost", "localhost");
        int esPort = Integer.parseInt(ConfigerationUtils.get("esPort", "19200"));

        String outputFilePath = ConfigerationUtils.get("outputFilePath", "D:\\Desktop\\");
        restClient = RestClient.builder(
                new HttpHost(esHost, esPort, "http"))
                .setMaxRetryTimeoutMillis(1000000)
                .build();

        File newFile = new File(outputFilePath + "/" + resultFileName);
        OutputStream os = new FileOutputStream(newFile);
        StringBuffer sb = new StringBuffer();

        String headLine = "索引名称,健康状态,索引uuid,主分片数,副本分片因子,文档数量,更新或删除过的文档数,实际存储,模板,分片配置合规性,刷新频率,是否建表超过180天,落盘配置";

        sb.append(headLine + "\n");
        //获取所有索引信息
        List<String> indicesInfoList = IndicesInfoRequestUtils.getIndicesList(restClient);
        Map<Pattern, JSONObject> templateMap = IndicesInfoRequestUtils.getTemplateMap(restClient);

        Pattern storeSizePattern = Pattern.compile("(([1-9]\\d*\\.?\\d+)|(0\\.\\d*[1-9])|(\\d+))(mb|kb|b)");
        Pattern storeTbSize = Pattern.compile("(([1-9]\\d*\\.?\\d+)|(0\\.\\d*[1-9])|(\\d+))(tb)");
        Pattern storeGbSize = Pattern.compile("(([1-9]\\d*\\.?\\d+)|(0\\.\\d*[1-9])|(\\d+))(gb)");


        Pattern dailyIndexPattern =  Pattern.compile("(\\d{4}_\\d{2}_\\d{2})|(\\d{8})");
//        Pattern dailyIndexPattern = Pattern.compile("((((19|20)\\d{2})_(0?[13-9]|1[012])_(0?[1-9]|[12]\\d|30))|" +
//                "(((19|20)\\d{2})_(0?[13578]|1[02])_31)|" +
//                "(((19|20)\\d{2})_0?2_(0?[1-9]|1\\d|2[0-8]))|" +
//                "((((19|20)([13579][26]|[2468][048]|0[48]))|(2000))_0?2_29)|" +
//                "(((19|20)\\d{2})(0?[13578]|1[02])31)|" +
//                "(((19|20)\\d{2})0?2(0?[1-9]|1\\d|2[0-8]))|" +
//                "((((19|20)([13579][26]|[2468][048]|0[48]))|(2000))0?229)|" +
//                ")");

        dataNodeNum = ClusterInfoUtils.countDataNodesNum(restClient);

        int dealing = 1;

        for(String indexInfo: indicesInfoList){
//
            String[] indexInfoSpli = indexInfo.split(",");
            if(indexInfoSpli.length == 10){
                String indexName = indexInfoSpli[2]; //索引名称
                String health = indexInfoSpli[0]; //健康状态
                String status = indexInfoSpli[1];

                String uuid = "'" + indexInfoSpli[3] + "'";//索引uuid
                int priNum = Integer.parseInt(indexInfoSpli[4]); //主分片数
                int repFactor = Integer.parseInt(indexInfoSpli[5]); //副本分片因子
                long docsCount = Long.parseLong(indexInfoSpli[6]); //文档数量
                long docsDeleted = Long.parseLong(indexInfoSpli[7]); //更新或删除过的文档数
                String storeSize = indexInfoSpli[8]; //实际存储
                String priStoreSize = indexInfoSpli[9]; //数据量存储大小
                System.out.println((dealing++) + ":" + indexName);

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

                Matcher tbSizeMatcher = storeTbSize.matcher(storeSize);
                Matcher gbSizeMatcher = storeGbSize.matcher(storeSize);
                Matcher smallSizeMatcher = storeSizePattern.matcher(storeSize);

                int shoudPartitionSize = dataNodeNum;
                if(gbSizeMatcher.matches()){ //末尾为gb
                    String gbNumStr = storeSize.substring(0,storeSize.lastIndexOf("gb"));
                    double gbNum = Double.parseDouble(gbNumStr);
                    if(gbNum <= 5){//数据量小于5G的索引，主分片数量配置为3.
                        if(priNum >=6){
                            shardsReq += "数据量小于5G的索引，主分片数量应配置小于等于6.";
                        }
                    }else if (5 < gbNum &&  gbNum <= 50){//数据量(n)大于5G小于等于50G，主分片数量配置为6.
                        if(priNum > 6){
                            shardsReq += "数据量大于5G的索引，小于50G时，主分片数量应配置小于等于6.";
                        }
                    }else{//数据量(n) 大于50G时，需满足：         a主分片数(shard×(1+副))    >= 集群数据节点数（data node） b每个分片数据量 （n/shard）小于30G c 主分片数量(shard)>6
                        shoudPartitionSize = (int)(gbNum/30) + 1 ;
                        if(priNum <= 6 ||//c主分片数量(shard)>6
                                priNum < shoudPartitionSize || // b每个分片数据量 （n/shard）小于30G
                                priNum *2 < dataNodeNum // a 分片数(shard×(1+副))    >= 集群数据节点数（data node）
                        ){
                            shardsReq += "数据量(n) 大于50G时，需满足： a主分片数(shard×(1+副)) >= 集群数据节点数（data node）； b每个分片数据量 （n/shard）小于30G； c 主分片数量(shard)>6。";
                        }

                    }

                }else if(tbSizeMatcher.matches()){//末尾为tb,均>5gb
                    String tbNumStr = storeSize.substring(0,storeSize.lastIndexOf("tb"));
                    double tbNum = Double.parseDouble(tbNumStr);
                    shoudPartitionSize = (int)(tbNum/0.03) + 1;
                    if(priNum <= 6 ||//c主分片数量(shard)>6
                            priNum < shoudPartitionSize || // b每个分片数据量 （n/shard）小于30G
                            priNum *2 < dataNodeNum // a 分片数(shard×(1+副))    >= 集群数据节点数（data node）
                    ){
                        shardsReq += "数据量(n) 大于50G时，需满足： a主分片数(shard×(1+副)) >= 集群数据节点数（data node）； b每个分片数据量 （n/shard）小于30G； c 主分片数量(shard)>6。";
                    }

                }else{ //末尾为b,kb,mb,的均小于5Gb,不会有pb的索引
                    if(priNum >=6){
                        shardsReq += "数据量小于5G的索引，主分片数量应配置小于等于6.";
                    }
                }


//                //3. **副本要求：**至少创建一份副本`number_of_replicas = 1`，防止集群某节点故障时主分片挂掉变为`red`状态，影响该索引查询。
//                String repReq = "";
//                if (repFactor == 0){
//                    repReq = "未包含副本分片，数据有丢失风险。";
//                }

                //4. **刷新要求：**频繁入库和频繁更新的索引要根据实际情况调整`refresh_interval`参数，至少为 `30s` 以上
                String refreshReq = "";
                //5. createionDate
                String creationDateReq = "";
//                //6. dailyIndex
//                String dailyIndexReq = "";
//
//                //7. 冷热节点 =""
//                String warmHotTypeReq = "";

                //8. translog设置
                String translogReq = "";

                JSONObject indexInfoJob = IndicesInfoRequestUtils.getIndexInfo(restClient, indexName);
                if (indexInfoJob!=null){
                    JSONObject settings = indexInfoJob.getJSONObject("settings");
                    JSONObject indexSettings = settings.getJSONObject("index");

                //4. **刷新要求：**频繁入库和频繁更新的索引要根据实际情况调整`refresh_interval`参数，至少为 `30s` 以上
                    if(!indexSettings.containsKey("refresh_interval")){
                        refreshReq = "未设置入库刷新频率";
                    }

                //5. 创建时间
                    Date creationDate = new Date(indexSettings.getLong("creation_date"));


                    int dateDifference = (int) ((today.getTime() - creationDate.getTime()) / (24 * 60 * 60 * 1000.0));

                    if(dateDifference >= 180){
                        creationDateReq = "建表日期：" + yyyyMMdd.format(creationDate) +"建表超过180天，请及时清理备份历史数据，关闭或删除无用索引";

                    }

//                //6. 天表索引
//                    Matcher dailyIndex = dailyIndexPattern.matcher(indexName);
//
//                    if(dailyIndex.find()){
//                        dailyIndexReq = "天表索引建议合并为月表";
//                    }

//                //7. 冷热节点
//                    warmHotTypeReq = "未包含（或未正确配置）冷热节点属性";
//                    if(indexSettings.containsKey("routing")){
//                        JSONObject routing = indexSettings.getJSONObject("routing");
//                        if(routing.containsKey("allocation")){
//                            JSONObject allocation = routing.getJSONObject("allocation");
//                            if(allocation.containsKey("require")){
//                                JSONObject require = allocation.getJSONObject("require");
//                                if(require.containsKey("box_type")){
//                                    warmHotTypeReq="";
//                                }
//                            }
//                        }
//                    }


                    //8. 写磁盘要求：**入库较为频繁的索引，需将`index.translog.durability`改为`async`，调大`index.translog.sync_interval=10s`，避免过多的flush写磁盘操作。

                    if(indexSettings.containsKey("translog")){
                        JSONObject translogSettings = indexSettings.getJSONObject("translog");
                        if(translogSettings.containsKey("durability")){
                            String durability = translogSettings.getString("durability");
                            if(!"async".equals(durability)){
                                translogReq += "未设置translog落盘配置为异步；";
                            }
                        }

                    }else{
                        translogReq += "未设置translog落盘配置；";
                    }
                }


//索引名称,健康状态,索引uuid,主分片数,副本分片因子,文档数量,更新或删除过的文档数,实际存储,模板,分片配置合规性,刷新频率,是否建表超过180天,落盘配置
                String lineStr =
                        indexName + "," //索引名称
                        + health + "," //健康状态
                        + uuid + "," //索引uuid
                        + priNum + "," //主分片数
                        + repFactor + "," //副本分片因子
                        + docsCount + "," //文档数量
                        + docsDeleted + "," //更新或删除过的文档数
                        + storeSize + "," //实际存储
                        + template + "," //模板
                        + shardsReq + "," //分片配置合规性
//                        + repReq + ","
                        + refreshReq + ","  //刷新频率
                        + creationDateReq + "," //是否建表超过180天
//                        + dailyIndexReq  + ","
//                        + warmHotTypeReq + ","
                        + translogReq + "," //落盘配置
                        + "\n";




                sb.append(lineStr);

            }else {
                indexInfo = indexInfo.replaceFirst(",","");
                String[] indexInfoSpliClose = indexInfo.split(",");
                indexInfo = indexInfoSpliClose[1] + ",," + indexInfoSpliClose[2] + ",";
                sb.append(indexInfo + "\n");
                System.out.println(indexInfo);
            }

        }


        os.write(sb.toString().getBytes());

        os.flush();
        os.close();
        restClient.close();

        try {
            //转换excel
            System.out.println("转换excel");
            Csv2Xlsx.convert(newFile);
        } catch (Exception e) {
            e.printStackTrace();
        }


    }






}
