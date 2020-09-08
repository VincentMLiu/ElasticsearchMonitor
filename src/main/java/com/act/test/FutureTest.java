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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class FutureTest {

    //获取系统核数
    static final int nThreads = Runtime.getRuntime().availableProcessors();

    public static void main(String[] args) {


        int n = nThreads; //默认线程为核数
        int bn = 2;
        int bs = 10;
        String number = nThreads + "";
        String batchNo = "2";
        String batchSize = "10";



        if(args.length == 2){
            batchNo = args[0]; //每个线程入多少批次
            batchSize = args[1];//每个线程每批入多少条
        }else if(args.length == 3){
            number = args[0]; ///总共多少个线程
            batchNo = args[1]; //每个线程入多少批次
            batchSize = args[2];//每个线程每批入多少条
        }else{
            System.err.println("参数个数必须为2或3");
            System.err.println("参数个数为2：batchNo  batchSize， 线程数为系统核数");
            System.err.println("参数个数为3：number batchNo  batchSize");
            System.exit(-1);
        }

        //总共多少个线程
        if(number!=null && !"".equals(number)){
            n = new Integer(number);
        }

        //每个线程入多少批次
        if(batchNo!=null && !"".equals(batchNo)){
            bn = new Integer(batchNo);
        }

        //每个线程每批入多少条
        if(batchSize!=null && !"".equals(batchSize)){
            bs = new Integer(batchSize);
        }



        Long start = System.currentTimeMillis();
        //开启多线程
        ExecutorService exs = Executors.newFixedThreadPool(n);
        try {
            //结果集
            List<Double> list = new ArrayList<>();
            List<Future<Double>> futureList = new ArrayList<>();

            //1.高速提交10个任务，每个任务返回一个Future入list
            for (int i = 0; i < n; i++) {
                futureList.add(exs.submit(new IndicesPressureTask(bn,bs)));
            }
            Long getResultStart = System.currentTimeMillis();
            System.out.println("结果归集开始时间=" + new Date());
            //2.结果归集，用迭代器遍历futureList,高速轮询（模拟实现了并发），任务完成就移除
            while(futureList.size()>0){
                Iterator<Future<Double>> iterable = futureList.iterator();
                //遍历一遍
                while(iterable.hasNext()){
                    Future<Double> future = iterable.next();
                    //如果任务完成取结果，否则判断下一个任务是否完成
                    if (future.isDone() && !future.isCancelled()){
                        //获取结果
                        Double i = future.get();
                        System.out.println("任务i=" + i + "获取完成，移出任务队列！" + new Date());
                        list.add(i);
                        //任务完成移除任务
                        iterable.remove();
                    }else{
                        Thread.sleep(1);//避免CPU高速运转，这里休息1毫秒，CPU纳秒级别
                    }
                }
            }
            System.out.println("list=" + list);
            System.out.println("总耗时=" + (System.currentTimeMillis() - start) + ",取结果归集耗时=" + (System.currentTimeMillis() - getResultStart));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            exs.shutdown();
        }
    }

    static class IndicesPressureTask implements Callable<Double> {
        int batchNo = 2;
        int batchSize = 10;


        private static SimpleDateFormat yyyyMMdd = new SimpleDateFormat("yyyyMMdd");
        private static SimpleDateFormat yyyyMMddHHmmss = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        private static SimpleDateFormat yyyy_MM_dd = new SimpleDateFormat("yyyy-MM-dd");

        private static RestClient restClient;

        public IndicesPressureTask(int batchNo, int batchSize) {
            super();
            this.batchNo = batchNo;
            this.batchSize = batchSize;
        }

        @Override
        public Double call() throws Exception {


            System.out.println("START TIME:" + yyyyMMddHHmmss.format(new Date()));
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

                    if(requestMode.equalsIgnoreCase("sync")){
                        response = restClient.performRequest("POST", "/_bulk", param, entity);

                        BufferedReader br = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));

                        StringBuffer sb = new StringBuffer();

                        String data;
                        while ((data = br.readLine()) != null) {
                            sb.append(data);
                        }

                        JSONObject syncResult = JSONObject.parseObject(sb.toString());
                        syncResult.get("took");//入库时间，毫秒数
                        syncResult.get("errors");//是否有报错
                        syncResult.getJSONArray("items"); //每条请求的返回resultlist

                        System.out.println(syncResult.toJSONString());
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
                                    syncResult.get("took");//入库时间，毫秒数
                                    syncResult.get("errors");//是否有报错
                                    syncResult.getJSONArray("items"); //每条请求的返回resultlist

                                    System.out.println(syncResult.toJSONString());
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }


                            }
                            @Override
                            public void onFailure(Exception exception) {
                                // 定义请求失败时需要做的事情，即每当发生连接错误或返回错误状态码时做的操作。
                            }
                        };
                        restClient.performRequestAsync("POST", "/_bulk", param, entity,responseListener);
                    }



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

            return gbResult;
        }
    }

}
