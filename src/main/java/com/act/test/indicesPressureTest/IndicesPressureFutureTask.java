package com.act.test.indicesPressureTest;

import com.act.ElasticsearchMonitor.elasticsearch.utils.ConfigerationUtils;
import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.RestClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

public class IndicesPressureFutureTask implements Callable<TaskResultBean> {


        int batchNo = 2;
        int batchSize = 10;


        private static SimpleDateFormat yyyyMMdd = new SimpleDateFormat("yyyyMMdd");
        private static SimpleDateFormat yyyyMMddHHmmss = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        private static SimpleDateFormat yyyy_MM_dd = new SimpleDateFormat("yyyy-MM-dd");

        private static RestClient restClient;

        private TaskResultBean taskResultBean;

        CountDownLatch latch;

        public IndicesPressureFutureTask(CountDownLatch latch, int batchNo, int batchSize) {
            super();
            this.latch = latch;
            this.batchNo = batchNo;
            this.batchSize = batchSize;
            taskResultBean = new TaskResultBean(Thread.currentThread().getName());
        }

        @Override
        public TaskResultBean call() throws Exception {

            //任务开始时间
            System.out.println("START TIME:" + yyyyMMddHHmmss.format(new Date()));
            long pressureSendSTARTTime = System.currentTimeMillis();//任务开始时间
            //ES连接信息
            ConfigerationUtils.init("esPressureTest.properties");
            String esHosts = ConfigerationUtils.get("esHosts", "localhost:19200");
            String requestMode = ConfigerationUtils.get("requestMode", "sync");
//        int esPort = Integer.parseInt(ConfigerationUtils.get("esPort", "19200"));
            String[] esHostsSpli = esHosts.split(",");

            String indexName = ConfigerationUtils.get("indexName", "dns_" + yyyyMMdd.format(new Date()) + "_activeip");
            String typeName = ConfigerationUtils.get("typeName", "activeip");


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

            long esDealingTotalTime = 0l;

            while( batchIndex < batchNo){

                long batchSendSTARTTime = System.currentTimeMillis();//任务开始时间
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

                    if(requestMode.equalsIgnoreCase("sync")){



                        response = restClient.performRequest("POST", "/_bulk", param, entity);

                        BufferedReader br = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));

                        StringBuffer sb = new StringBuffer();

                        String data;
                        while ((data = br.readLine()) != null) {
                            sb.append(data);
                        }

                        JSONObject syncResult = JSONObject.parseObject(sb.toString());
                        long esDealingTime = Long.parseLong(syncResult.get("took").toString());//入库时间，毫秒数
                        esDealingTotalTime += esDealingTime;
                        syncResult.get("errors");//是否有报错
                        syncResult.getJSONArray("items"); //每条请求的返回resultlist

//                        System.out.println(syncResult.toJSONString());


                        latch.countDown();
                    }else if(requestMode.equalsIgnoreCase("async")){

                        ResponseListener responseListener = new ResponseListener() {
                            @Override
                            public void onSuccess(Response response) {
                                // 定义请求成功执行时需要做的事情
                                BufferedReader br = null;
                                try {
                                    br = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
                                    StringBuffer sb = new StringBuffer();

                                    String data;
                                    while ((data = br.readLine()) != null) {
                                        sb.append(data);
                                    }
                                    JSONObject syncResult = JSONObject.parseObject(sb.toString());
                                    long esDealingTime = Long.parseLong(syncResult.get("took").toString());//入库时间，毫秒数
                                    System.out.println("[NOW] - " + Thread.currentThread().getName() + " esDealingTime: " + esDealingTime);
                                    syncResult.get("errors");//是否有报错
                                    syncResult.getJSONArray("items"); //每条请求的返回resultlist

//                                    System.out.println(syncResult.toJSONString());
                                    latch.countDown();
                                } catch (IOException e) {
                                    e.printStackTrace();
                                    latch.countDown();
                                }


                            }
                            @Override
                            public void onFailure(Exception exception) {
                                // 定义请求失败时需要做的事情，即每当发生连接错误或返回错误状态码时做的操作。
                                exception.printStackTrace();

                                latch.countDown();
                            }
                        };
                        restClient.performRequestAsync("POST", "/_bulk", param, entity,responseListener);

                    }



                } catch (IOException e) {
                    e.printStackTrace();
                }

                batchIndex++;


                long batchSendENDTime = System.currentTimeMillis();//每批结束时间
                long batchSendTime = batchSendENDTime - batchSendSTARTTime;
                System.out.println("[NOW] - " + Thread.currentThread().getName() + " : " + (index -1));
                System.out.println("[NOW] - " + Thread.currentThread().getName() + " batchSendTime: " + batchSendTime);


            }


            //任务结束时间
            System.out.println("END TIME:" + yyyyMMddHHmmss.format(new Date()));
            long pressureSendENDTime = System.currentTimeMillis();//任务结束时间
            long pressureSendTime = pressureSendENDTime - pressureSendSTARTTime;
            System.out.println("[FINAL] - " + Thread.currentThread().getName() + " - totalRequestTime: " + pressureSendTime);
            taskResultBean.setSendTimeInMils(pressureSendTime);
            //总发送量
            System.out.println("[FINAL] - " + Thread.currentThread().getName() + " : " + (index -1));
            double gbResult = ((double)totalBytes)/1024.0/1024.0/1024.0;
            System.out.println("[FINAL] - " + Thread.currentThread().getName() + " - totalRequestSize(GB): " + gbResult);

            //结果集

            taskResultBean.setSendCount(index-1);
            taskResultBean.setEsDealingTimeInMils(esDealingTotalTime);
            taskResultBean.setGbSize(gbResult);


            restClient.close();

            return taskResultBean;
        }

}
