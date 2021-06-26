package com.act.test;

import com.act.ElasticsearchMonitor.elasticsearch.utils.ConfigerationUtils;
import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.RestClient;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

public class T_QXRZ_TEST extends Thread {

    private static RestClient restClient;


    //获取系统核数
    static final int nThreads = Runtime.getRuntime().availableProcessors();


        public static void main(String[] args) throws IOException {

            String nodeListStr = "node-172.16.16.44,node-172.16.16.42,node-172.16.16.43,node-172.16.16.33,node-172.16.16.58,node-172.16.16.38,node-172.16.16.40,node-172.16.16.41,node-172.16.16.34,node-172.16.16.39,node-172.16.16.56,node-172.16.16.35,node-172.16.16.37";

            List<String> nodeList = Arrays.asList(nodeListStr.split(","));

//
//            String log = "17 十一月 2020 15:25:12,214 INFO  [PollableSourceRunner-NeiMengKafkaConsumer0_10_1Flume1_6-skafka] (com.act.flume.source.kafka.NeiMengKafkaConsumer0_10_1Flume1_6.seperateIntercept:510)  - {\"c_capturetime\": 1605597809, \"c_sip\": \"10.232.76.251\", \"c_dip\": \"111.13.234.113\", \"c_sport\": 56684, \"c_dport\": 80, \"c_protocol\": 6, \"c_l5_proto\": 0, \"c_network_proto\": 0, \"c_tunnel_sip\": \"10.129.24.84\", \"c_tunnel_dip\": \"100.88.255.53\", \"c_uplink_teid\": 298370153, \"c_downlink_teid\": 809199288, \"c_stream_stime\": 1605597809, \"c_stream_etime\": 1605597809, \"c_last_time\": 0, \"c_flow_dir\": 1, \"c_busstype\": 181, \"c_nettype\": 2, \"c_protype\": 1, \"c_mime_type\": 1, \"c_ydz_spcode\": 1, \"c_ydz_ascode\": 4, \"c_u_jmflag\": 0, \"c_msisdn\": \"\", \"c_imsi\": \"\", \"c_imei\": \"\", \"c_localip\": \"10.70.0.3\", \"c_url\": \"api.afdback.ppsimg.com\\/dom?bsid=core\", \"c_inputtime\": 0, \"c_ydz_lac\": 0, \"c_ydz_ci\": 0, \"c_ydz_rac\": 0, \"c_ydz_areacode\": 0, \"c_ydz_homecode\": 0, \"c_apn\": \"\", \"c_ydz_uli\": \"\", \"c_lat\": \"\", \"c_lng\": \"\", \"c_uli_addr\": \"\", \"c_qd_szs\": 0, \"c_upstream_pkts\": 5, \"c_upstream_bytes\": 827, \"c_downstream_pkts\": 4, \"c_downstream_bytes\": 1333, \"c_extern_content\": {\"APP\": \"181\", \"HOST\": \"api.afdback.ppsimg.com\", \"CONTENT_LENGTH\": \"0\", \"USER_AGENT\": \"QYPlayer\\/Android\\/4.7.1701;BT\\/mcto;Pt\\/Mobile;NetType\\/unknown;Hv\\/;QTP\\/2.1.56.2\", \"CONTENT_TYPE\": \"application\\/json\"}}";
//            System.out.println(log.indexOf("{\"c_capturetime\""));
//            String jsonString = log.substring(log.indexOf("{\"c_capturetime\""));
//            System.out.println(jsonString);
//            JSONObject job = JSONObject.parseObject(jsonString);
//            System.out.println(job.toJSONString());


            //ES连接信息
            String esHosts = "localhost:19200";
            String requestMode = "sync";
            String[] esHostsSpli = esHosts.split(",");


            HttpHost[] HttpHostArray = new HttpHost[esHostsSpli.length];

            for (int i = 0; i < esHostsSpli.length; i++) {
                String[] esHostSpli = esHostsSpli[i].split(":");
                if (esHostSpli.length == 2) {
                    String host = esHostSpli[0];
                    int port = Integer.parseInt(esHostSpli[1]);
                    HttpHostArray[i] = new HttpHost(host, port, "http");
                }
            }
//            String esHost = "localhost";
//            int esPort = 19202;
            if (HttpHostArray[0] != null) {
                restClient = RestClient.builder(
                        HttpHostArray)
                        .setMaxRetryTimeoutMillis(1000000)
                        .build();
            } else {
                System.err.println("At list config one client node");
                System.exit(-1);
            }


            File logFile = new File("D:\\Desktop\\flume-t_qxrz_topic_kafka_0_10_1_fraud7081_19206.log");

            BufferedReader br = new BufferedReader(new FileReader(logFile));

            String lineStr = "";
            StringBuffer jb = new StringBuffer();

            while ((lineStr = br.readLine()) != null) {
                int startIndex = lineStr.indexOf("{\"c_capturetime\"");
                if(startIndex < 0){
                    continue;
                }

                //{ "index" : { "_index" : "test", "_type" : "type1", "_id" : "1" } }

                JSONObject indexRequest = new JSONObject();
                indexRequest.put("_index", "t_qxrz_search");
                indexRequest.put("_type", "t_qxrz");

                JSONObject outObj = new JSONObject();
                outObj.put("index", indexRequest);
                jb.append(outObj.toJSONString() + "\n");


                JSONObject logJob = JSONObject.parseObject(lineStr.substring(startIndex));

                jb.append(logJob.toJSONString() + "\n");
            }


            System.out.println(jb.toString());
            System.out.println("buffer size: " + jb.length());

            Map<String, String> param = new HashMap<>();

            param.put("pretty", "true");


            HttpEntity entity = new NStringEntity(jb.toString(), ContentType.APPLICATION_JSON);

            long ctBytes = entity.getContentLength();

            Response response = null;
            try {

                if (requestMode.equalsIgnoreCase("sync")) {
                    response = restClient.performRequest("POST", "/_bulk", param, entity);
                } else if (requestMode.equalsIgnoreCase("async")) {

                    ResponseListener responseListener = new ResponseListener() {
                        @Override
                        public void onSuccess(Response response) {
                            // 定义请求成功执行时需要做的事情
                        }

                        @Override
                        public void onFailure(Exception exception) {
                            // 定义请求失败时需要做的事情，即每当发生连接错误或返回错误状态码时做的操作。
                        }
                    };
                    restClient.performRequestAsync("POST", "/_bulk", param, entity, responseListener);
                }


//                response = restClient.performRequest("POST", "/_bulk", param, entity);
                BufferedReader brR = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));

                StringBuffer sb = new StringBuffer();

                String data;
                while ((data = brR.readLine()) != null) {
                    sb.append(data);
                }

                System.out.println(sb.toString());

            } catch (IOException e) {
                e.printStackTrace();
            }


            restClient.close();
        }




}
