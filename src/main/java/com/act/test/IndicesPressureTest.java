package com.act.test;

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

public class IndicesPressureTest {


    private static SimpleDateFormat yyyyMMdd = new SimpleDateFormat("yyyyMMdd");

    private static RestClient restClient;

    private static int dataNodeNum = 0;

    //获取系统核数
    static final int nThreads = Runtime.getRuntime().availableProcessors();

    private static Date today = new Date();


        public static void main(String[] args) throws IOException {


            //ES连接信息
//            ConfigerationUtils.init("indicesDelete.properties");
//            String esHost = ConfigerationUtils.get("esHost", "localhost");
//            int esPort = Integer.parseInt(ConfigerationUtils.get("esPort", "19200"));
            String esHost = "localhost";
            int esPort = 19202;
//            restClient = RestClient.builder(
//                    new HttpHost(esHost, esPort, "http"))
//                    .setMaxRetryTimeoutMillis(1000000)
//                    .build();

            String nodeListStr = "node-172.16.16.44,node-172.16.16.42,node-172.16.16.43,node-172.16.16.33,node-172.16.16.58,node-172.16.16.38,node-172.16.16.40,node-172.16.16.41,node-172.16.16.34,node-172.16.16.39,node-172.16.16.56,node-172.16.16.35,node-172.16.16.37";

            List<String> nodeList = Arrays.asList(nodeListStr.split(","));

            String indexName = "IndicesPressureTest_" + yyyyMMdd.format(new Date());
            String typeName = "IndicesPressureTest";

            int numOfBatch = 5; //发送批次
            int numOfBatchSize = 10; //每批发送的数据条数


            Random rd = new Random();

            long index = 1l;//总处理数
            int batchIndex = 0;//批次游标

            StringBuffer jb = new StringBuffer();

            while( batchIndex < numOfBatch){
                JSONArray commands = new JSONArray();

                int perBatchIndex = 0;

                while( perBatchIndex < numOfBatchSize ){
                    //{ "index" : { "_index" : "test", "_type" : "type1", "_id" : "1" } }

                    JSONObject indexRequest = new JSONObject();
                    indexRequest.put("_index", indexName);
                    indexRequest.put("_type", typeName);
                    indexRequest.put("_id", System.currentTimeMillis() + "" + rd.nextLong());

                    jb.append(indexRequest.toJSONString() + "\n");
                    //{ "field1" : "value1" }
                    JSONObject fieldRequest = new JSONObject();
                    fieldRequest.put("column1", "column1-" + index);
                    fieldRequest.put("column2", "column2-" + index);
                    fieldRequest.put("column3", "column3-" + index);
                    fieldRequest.put("column4", "column4-" + index);
                    fieldRequest.put("column5", "column5-" + index);

                    jb.append(fieldRequest.toJSONString() + "\n");
                    perBatchIndex++;
                    index++;
                }
                batchIndex++;


            }



            System.out.println(jb.toString());

            Map<String,String> param = new HashMap<>();

            param.put("pretty", "true");


            HttpEntity entity = new NStringEntity(jb.toString(), ContentType.APPLICATION_JSON);
            Response response = restClient.performRequest("POST", "_bulk", param, entity);

            BufferedReader br = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));

            StringBuffer sb = new StringBuffer();

            String data;
            while ((data = br.readLine()) != null) {
                sb.append(data);
            }

            System.out.println(sb.toString());
        }




}
