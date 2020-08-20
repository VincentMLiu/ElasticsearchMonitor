package com.act.ElasticsearchMonitor.elasticsearch;

import com.act.ElasticsearchMonitor.elasticsearch.utils.ClusterInfoUtils;
import com.act.ElasticsearchMonitor.elasticsearch.utils.ConfigerationUtils;
import com.act.ElasticsearchMonitor.elasticsearch.utils.IndicesInfoRequestUtils;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IndicesDelete {


    private static SimpleDateFormat yyyyMMdd = new SimpleDateFormat("yyyy-MM-dd");

    private static RestClient restClient;

    private static int dataNodeNum = 0;


    private static Date today = new Date();

    public static void main(String[] args) throws IOException {

        //ES连接信息
        ConfigerationUtils.init("indicesDelete.properties");
        String esHost = ConfigerationUtils.get("esHost", "localhost");
        int esPort = Integer.parseInt(ConfigerationUtils.get("esPort", "19200"));
        restClient = RestClient.builder(
                new HttpHost(esHost, esPort, "http"))
                .setMaxRetryTimeoutMillis(1000000)
                .build();
        //索引留存信息
        Map<Pattern, Integer> indexReserveInfo = new HashMap<Pattern, Integer>();
        BufferedReader br =  new BufferedReader(new FileReader(ConfigerationUtils.get("indicesPatternsPath", "/home/elastic/EsMonitor/conf/indicesPatterns.tb")));
        String line = "";
        while(StringUtils.isNotBlank(line = br.readLine())){
            String[] lineSpli = line.split("=");
            if(lineSpli.length == 2){
                //配置有误，直接抛错
                Pattern indexPettern = Pattern.compile(lineSpli[0].replace("*","\\w+"));
                int reserveDate = Integer.parseInt(lineSpli[1]);
                indexReserveInfo.put(indexPettern, reserveDate);
            }
        }

        //获取所有索引信息
        List<String> indicesInfoList = IndicesInfoRequestUtils.getIndicesList(restClient);

        int dealing = 1;
        for(String indexInfo: indicesInfoList){
            String[] indexInfoSpli = indexInfo.split(",");
            if(indexInfoSpli.length == 10 || indexInfoSpli.length == 4){
                String indexName = "";
                String health = "";
                if(indexInfoSpli.length == 10){
                    health = indexInfoSpli[0];
                    indexName = indexInfoSpli[2];
                }else if(indexInfoSpli.length == 4){
                    indexName = indexInfoSpli[2];
                }

//                System.out.println((dealing++) + ":" + indexName);

                if(indexName.equals(".kibana")){
                    continue;
                }

                //createionDate
                String creationDateReq = "";
                //dailyIndex
                String dailyIndexReq = "";

                JSONObject indexInfoJob = IndicesInfoRequestUtils.getIndexInfo(restClient, indexName);
                if (indexInfoJob!=null){
                    int reserveDate = 99999999;
                    for(Pattern pt : indexReserveInfo.keySet()){
                        Matcher dailyIndex = pt.matcher(indexName);
                        if(dailyIndex.matches()){
                            reserveDate = indexReserveInfo.get(pt);
                            break;
                        }
                    }


                    JSONObject settings = indexInfoJob.getJSONObject("settings");
                    JSONObject indexSettings = settings.getJSONObject("index");



                //创建时间
                    Date creationDate = new Date(indexSettings.getLong("creation_date"));

                    int dateDifference = (int) ((today.getTime() - creationDate.getTime()) / (24 * 60 * 60 * 1000.0));

                    if(dateDifference >= reserveDate){
                        creationDateReq = "[" + indexName + "] 建表日期：" + yyyyMMdd.format(creationDate) +"建表超过 " + reserveDate + " 天，准备删除";
                        JSONObject acknowledged = IndicesInfoRequestUtils.deleteIndex(restClient,indexName);


                        System.out.print(dealing + " : " + creationDateReq );

                        if(acknowledged!=null){
                            if(acknowledged.getString("acknowledged")!=null && "true".equalsIgnoreCase(acknowledged.getString("acknowledged"))){
                                System.out.println("    结果：" + acknowledged.toString());
                                dealing++;
                            }else {
                                System.out.println("    结果：" + acknowledged.toString());
                            }
                        }else{
                            System.out.println("    结果：" + "false");
                        }
                    }else{
//                        creationDateReq = "[" + indexName + "] 建表日期：" + yyyyMMdd.format(creationDate) +"建表" + dateDifference + "天, 继续保留";
                    }

                }

            }else {
                System.out.println(indexInfo);
            }

        }

        System.out.println("Total remove indices : "  + dealing);
        restClient.close();
    }

}
