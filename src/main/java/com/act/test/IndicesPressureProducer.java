package com.act.test;

import com.act.ElasticsearchMonitor.elasticsearch.utils.ConfigerationUtils;
import com.act.ElasticsearchMonitor.elasticsearch.utils.IndicesInfoRequestUtils;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.*;

public class IndicesPressureProducer extends Thread {


    private static SimpleDateFormat yyyyMMdd = new SimpleDateFormat("yyyyMMdd");
    private static SimpleDateFormat yyyyMMddHHmmss = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static SimpleDateFormat yyyy_MM_dd = new SimpleDateFormat("yyyy-MM-dd");

    private static RestClient restClient;


    //获取系统核数
    static final int nThreads = Runtime.getRuntime().availableProcessors();

    private static Date today = new Date();

    int number = nThreads;
    int batchNo = 10;
    int batchSize = 100000;


    public IndicesPressureProducer(int number, int batchNo, int batchSize){
        this.number = number;
        this.batchNo = batchNo;
        this.batchSize = batchSize;
    }



    @Override
    public void run() {

        System.out.println("START TIME:" + yyyyMMddHHmmss.format(new Date()));
        //ES连接信息
        ConfigerationUtils.init("esPressureTest.properties");
        String esHosts = ConfigerationUtils.get("esHosts", "localhost:19200");
//        int esPort = Integer.parseInt(ConfigerationUtils.get("esPort", "19200"));
        String[] esHostsSpli = esHosts.split(",");

        String indexName = ConfigerationUtils.get("indexName", "dns_" + yyyyMMdd.format(new Date()) + "_activeip");
        String typeName = ConfigerationUtils.get("typeName", "ACTIVEIP");


        HttpHost[] HttpHostArray = new HttpHost[esHostsSpli.length];

        for(int i = 0 ; i < esHostsSpli.length; i++){
            String[] esHostSpli = esHostsSpli[i].split(":");
            if(esHostSpli.length ==2){
                String host = esHostSpli[0];
                int port = Integer.parseInt(esHostSpli[1]);
                HttpHostArray[i] = new HttpHost(host, port, "http");
            }
        }
//            String esHost = "localhost";
//            int esPort = 19202;
        if(HttpHostArray[0] != null){
            restClient = RestClient.builder(
                    HttpHostArray)
                    .setMaxRetryTimeoutMillis(1000000)
                    .build();
        }else{
            System.err.println("At list config one client node");
            System.exit(-1);
        }


        Random rd = new Random();

        long index = 1l;//总处理数
        int batchIndex = 0;//批次游标


        long totalBytes = 0;

        while( batchIndex < batchNo){

            StringBuffer jb = new StringBuffer();
            int perBatchIndex = 0;

            String timeString = yyyyMMddHHmmss.format(new Date());
            String dateStr = yyyy_MM_dd.format(new Date());

            while( perBatchIndex < batchSize ){
                //{ "index" : { "_index" : "test", "_type" : "type1", "_id" : "1" } }

                JSONObject indexRequest = new JSONObject();
                indexRequest.put("_index", indexName);
                indexRequest.put("_type", typeName);
//                indexRequest.put("_id", System.currentTimeMillis() + "" + rd.nextLong());

                JSONObject outObj = new JSONObject();
                outObj.put("index", indexRequest);

                jb.append(outObj.toJSONString() + "\n");
                //{ "field1" : "value1" }
                JSONObject fieldRequest = new JSONObject();
                fieldRequest.put("lastTime", timeString);
                fieldRequest.put("dateTime", dateStr);
                fieldRequest.put("houseId", index);
                fieldRequest.put("ip", "p14.29.48." + index);
                fieldRequest.put("idcIspName", "中国电信股份有限公司广东分公司-" + index);
                fieldRequest.put("firstTime", timeString);
                fieldRequest.put("visitsCount", index);
                fieldRequest.put("houseName", "佛山本地IDC中心-" + index);
                fieldRequest.put("isInIpSeg", 0);
                fieldRequest.put("protocol", 0);
                fieldRequest.put("isSpecial", 0);
                fieldRequest.put("port", index);
                fieldRequest.put("idcId", "A2.B1.B2-20090001-" + index);
                fieldRequest.put("createTime", timeString);
                fieldRequest.put("lastDay", index);
                fieldRequest.put("block", 0);
                fieldRequest.put("key", "612100920623439995" + index);

                jb.append(fieldRequest.toJSONString() + "\n");
                perBatchIndex++;
                index++;
            }



//            System.out.println(jb.toString());
//            System.out.println("buffer size: " + jb.length());

            Map<String,String> param = new HashMap<>();

            param.put("pretty", "true");


            HttpEntity entity = new NStringEntity(jb.toString(), ContentType.APPLICATION_JSON);

            long ctBytes = entity.getContentLength();

            totalBytes = totalBytes + ctBytes;
//            System.out.println("entity size: " + ctBytes);

            Response response = null;
            try {
                response = restClient.performRequest("POST", "/_bulk", param, entity);
//                BufferedReader br = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
//
//                StringBuffer sb = new StringBuffer();
//
//                String data;
//                while ((data = br.readLine()) != null) {
//                    sb.append(data);
//                }
//
//                System.out.println(sb.toString());
            } catch (IOException e) {
                e.printStackTrace();
            }

            batchIndex++;


            System.out.println("[NOW] - " + Thread.currentThread().getName() + " : " + (index -1));


        }


        double gbResult = ((double)totalBytes)/1024.0/1024.0/1024.0;

        System.out.println("END TIME:" + yyyyMMddHHmmss.format(new Date()));
        System.out.println("[FINAL] - " + Thread.currentThread().getName() + " : " + (index -1));
        System.out.println("[FINAL] - " + Thread.currentThread().getName() + " - totalRequestSize(GB): " + gbResult);

        Thread.currentThread().interrupt();

    }

        public static void main(String[] args) throws IOException {

            String nodeListStr = "node-172.16.16.44,node-172.16.16.42,node-172.16.16.43,node-172.16.16.33,node-172.16.16.58,node-172.16.16.38,node-172.16.16.40,node-172.16.16.41,node-172.16.16.34,node-172.16.16.39,node-172.16.16.56,node-172.16.16.35,node-172.16.16.37";

            List<String> nodeList = Arrays.asList(nodeListStr.split(","));

            int numOfBatch = 5; //发送批次
            int numOfBatchSize = 10; //每批发送的数据条数





        }




}
