package com.act.ElasticsearchMonitor.elasticsearch;

import com.act.ElasticsearchMonitor.elasticsearch.utils.ClusterInfoUtils;
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

        String host = "localhost";
        int port = 19201;
        String outputFilePath = "C:\\Users\\ThinkPad\\Desktop\\es集群信息 " + yyyyMMdd.format(new Date()) +".csv";

        if(args.length == 2){
            String[] hostSpli = args[0].split(":");
            host = hostSpli[0];
            port = Integer.parseInt(hostSpli[1]);

            outputFilePath = args[1];
        }


        restClient = RestClient.builder(
                new HttpHost(host, port, "http"))
                .setMaxRetryTimeoutMillis(1000000)
                .build();

        File newFile = new File(outputFilePath);
        OutputStream os = new FileOutputStream(newFile);
        StringBuffer sb = new StringBuffer();

        String headLine = "索引状态,索引名称,索引uuid,主分片数,副本分片因子,文档数量,更新或删除过的文档数,实际存储,模板,分片配置合规性,是否包含副本分片,是否设置了刷新频率,是否建表超过180天,是否为天表索引";

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

                String health = indexInfoSpli[0];
                String status = indexInfoSpli[1];
                String indexName = indexInfoSpli[2];
                String uuid = "'" + indexInfoSpli[3] + "'";
                int priNum = Integer.parseInt(indexInfoSpli[4]);
                int repFactor = Integer.parseInt(indexInfoSpli[5]);
                long docsCount = Long.parseLong(indexInfoSpli[6]);
                long docsDeleted = Long.parseLong(indexInfoSpli[7]);
                String storeSize = indexInfoSpli[8];
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
                if(gbSizeMatcher.matches()){
                    String gbNumStr = storeSize.substring(0,storeSize.lastIndexOf("gb"));
                    double gbNum = Double.parseDouble(gbNumStr);
                    shoudPartitionSize = (int)(gbNum/50) + 1 ;
                }else if(tbSizeMatcher.matches()){
                    String tbNumStr = storeSize.substring(0,storeSize.lastIndexOf("tb"));
                    double tbNum = Double.parseDouble(tbNumStr);
                    shoudPartitionSize = (int)(tbNum/0.05) + 1;
                }

                if( priNum > shoudPartitionSize){
                    shardsReq += "分片数量过多，主分片数(" + priNum + ")大于 (" + storeSize + "/50gb + 1);";
                }
                if(smallSizeMatcher.matches() && priNum >= 3){
                    shardsReq += "分片数量过多，主分片数(" + priNum + ")大于 (" + storeSize + "/50gb + 1);";
                }

                //3. **副本要求：**至少创建一份副本`number_of_replicas = 1`，防止集群某节点故障时主分片挂掉变为`red`状态，影响该索引查询。
                String repReq = "";
                if (repFactor == 0){
                    repReq = "未包含副本分片，数据有丢失风险。";
                }

                //4. **刷新要求：**频繁入库和频繁更新的索引要根据实际情况调整`refresh_interval`参数，至少为 `30s` 以上
                String refreshReq = "";
                //5. createionDate
                String creationDateReq = "";
                //6. dailyIndex
                String dailyIndexReq = "";

                //7. 冷热节点 =""
                String warmHotTypeReq = "";


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

                //6. 天表索引
                    Matcher dailyIndex = dailyIndexPattern.matcher(indexName);

                    if(dailyIndex.find()){
                        dailyIndexReq = "天表索引建议合并为月表";
                    }

                //7. 冷热节点
                    warmHotTypeReq = "未包含（或未正确配置）冷热节点属性";
                    if(indexSettings.containsKey("routing")){
                        JSONObject routing = indexSettings.getJSONObject("routing");
                        if(routing.containsKey("allocation")){
                            JSONObject allocation = routing.getJSONObject("allocation");
                            if(allocation.containsKey("require")){
                                JSONObject require = allocation.getJSONObject("require");
                                if(require.containsKey("box_type")){
                                    warmHotTypeReq="";
                                }
                            }
                        }
                    }


                }



                String lineStr =
                          status + ","
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
                        + refreshReq + ","
                        + creationDateReq + ","
                        + dailyIndexReq  + ","
                        + warmHotTypeReq + ","
                        + "\n";




                sb.append(lineStr);

            }else {
                indexInfo = indexInfo.replaceFirst(",","");
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
