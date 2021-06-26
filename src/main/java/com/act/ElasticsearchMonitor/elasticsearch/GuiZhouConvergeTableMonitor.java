package com.act.ElasticsearchMonitor.elasticsearch;

import com.act.ElasticsearchMonitor.elasticsearch.utils.ConfigerationUtils;
import com.act.ElasticsearchMonitor.elasticsearch.utils.IndicesInfoRequestUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.*;

public class GuiZhouConvergeTableMonitor {


    private static SimpleDateFormat yyyyMMddHH = new SimpleDateFormat("yyyyMMddHH");

    private static RestClient restClient;

    private static int dataNodeNum = 0;


    private static Date today = new Date();


        public static void main(String[] args) throws IOException {

            //ES连接信息
            ConfigerationUtils.init("guizhouMonitor.properties");
            String esHost = ConfigerationUtils.get("esHost", "172.17.0.37");
            int esPort = Integer.parseInt(ConfigerationUtils.get("esPort", "9202"));
            restClient = RestClient.builder(
                    new HttpHost(esHost, esPort, "http"))
                    .setMaxRetryTimeoutMillis(1000000)
                    .build();


            StringBuffer sb;
            JSONObject job;
            BufferedReader br;
            Response responseTemplate;
            String data;



            Map<String, String> params = Collections.singletonMap("pretty", "true");
            HttpEntity aggUrl = new NStringEntity(
                    "{ \"size\" : 0, \"aggs\": { \"result_source\": { \"terms\": { \"field\": \"result_source\", \"size\": 10 } } } }", ContentType.APPLICATION_JSON
            );

            JSONObject aggregations;
            JSONObject result_source;
            JSONArray buckets;

            responseTemplate = restClient.performRequest("GET", "t_result_converge_url/_search?", params, aggUrl);
            br = new BufferedReader(new InputStreamReader(responseTemplate.getEntity().getContent()));
            sb = new StringBuffer();

            while ((data = br.readLine()) != null) {
                sb.append(data);
            }
            job = JSON.parseObject(sb.toString());

            System.out.println(job.toJSONString());

            aggregations = job.getJSONObject("aggregations");
            result_source = aggregations.getJSONObject("result_source");
            buckets = result_source.getJSONArray("buckets");

            int actCountUrl = 0;
            int aliCountUrl = 0;
            int commonCountUrl = 0;

            for(int i = 0; i < buckets.size() ; ++i){

                JSONObject bucket = buckets.getJSONObject(i);
                String key = bucket.get("key").toString();
                int doc_count = Integer.parseInt(bucket.get("doc_count").toString());

                switch (key) {
                    case "1":
                        actCountUrl = doc_count;
                        break;
                    case "2":
                        aliCountUrl = doc_count;
                        break;
                    case "1|2":
                        commonCountUrl = doc_count;
                        break;
                }

            }


            responseTemplate = restClient.performRequest("GET", "t_result_converge_host/_search?", params, aggUrl);
            br = new BufferedReader(new InputStreamReader(responseTemplate.getEntity().getContent()));
            sb = new StringBuffer();

            while ((data = br.readLine()) != null) {
                sb.append(data);
            }
            job = JSON.parseObject(sb.toString());

            System.out.println(job.toJSONString());

            aggregations = job.getJSONObject("aggregations");
            result_source = aggregations.getJSONObject("result_source");
            buckets = result_source.getJSONArray("buckets");

            int actCountHost = 0;
            int aliCountHost = 0;
            int commonCountHost = 0;

            for(int i = 0; i < buckets.size() ; ++i){

                JSONObject bucket = buckets.getJSONObject(i);
                String key = bucket.get("key").toString();
                int doc_count = Integer.parseInt(bucket.get("doc_count").toString());

                switch (key) {
                    case "1":
                        actCountHost = doc_count;
                        break;
                    case "2":
                        aliCountHost = doc_count;
                        break;
                    case "1|2":
                        commonCountHost = doc_count;
                        break;
                }

            }


            responseTemplate = restClient.performRequest("GET", "t_result_converge_app/_search?", params, aggUrl);
            br = new BufferedReader(new InputStreamReader(responseTemplate.getEntity().getContent()));
            sb = new StringBuffer();

            while ((data = br.readLine()) != null) {
                sb.append(data);
            }
            job = JSON.parseObject(sb.toString());

            System.out.println(job.toJSONString());

            aggregations = job.getJSONObject("aggregations");
            result_source = aggregations.getJSONObject("result_source");
            buckets = result_source.getJSONArray("buckets");

            int actCountApp = 0;
            int aliCountApp = 0;
            int commonCountApp = 0;

            for(int i = 0; i < buckets.size() ; ++i){

                JSONObject bucket = buckets.getJSONObject(i);
                String key = bucket.get("key").toString();
                int doc_count = Integer.parseInt(bucket.get("doc_count").toString());

                switch (key) {
                    case "1":
                        actCountApp = doc_count;
                        break;
                    case "2":
                        aliCountApp = doc_count;
                        break;
                    case "1|2":
                        commonCountApp = doc_count;
                        break;
                }

            }


            responseTemplate = restClient.performRequest("GET", "t_result_converge_msg/_search?", params, aggUrl);
            br = new BufferedReader(new InputStreamReader(responseTemplate.getEntity().getContent()));
            sb = new StringBuffer();

            while ((data = br.readLine()) != null) {
                sb.append(data);
            }
            job = JSON.parseObject(sb.toString());

            System.out.println(job.toJSONString());

            aggregations = job.getJSONObject("aggregations");
            result_source = aggregations.getJSONObject("result_source");
            buckets = result_source.getJSONArray("buckets");

            int actCountMsg = 0;
            int aliCountMsg = 0;

            for(int i = 0; i < buckets.size() ; ++i){

                JSONObject bucket = buckets.getJSONObject(i);
                String key = bucket.get("key").toString();
                int doc_count = Integer.parseInt(bucket.get("doc_count").toString());

                switch (key) {
                    case "1":
                        actCountMsg = doc_count;
                        break;
                    case "2":
                        aliCountMsg = doc_count;
                        break;
                }

            }


            String param = "{ \"msgtype\": \"markdown\", \"markdown\": { " +
                    "\"content\": \"\\n" +
                    " # 统计时间 ： " + yyyyMMddHH.format(new Date()) + " \\n" +
                    " ## 亚鸿阿里汇聚表 url 统计\\n " +
                    " >亚鸿:<font color=\\\"comment\\\"> " + actCountUrl + " </font> \\n" +
                    " >阿里:<font color=\\\"comment\\\"> " + aliCountUrl + "</font> \\n" +
                    " >共有:<font color=\\\"comment\\\"> " + commonCountUrl + " </font> \\n" +
                    "\\n" +
                    " ## 亚鸿阿里汇聚表 host 统计\\n " +
                    " >亚鸿:<font color=\\\"comment\\\"> " + actCountHost + " </font> \\n" +
                    " >阿里:<font color=\\\"comment\\\"> " + aliCountHost + "</font> \\n" +
                    " >共有:<font color=\\\"comment\\\"> " + commonCountHost + " </font> \\n" +
                    "\\n" +
                    " ## 亚鸿阿里汇聚表 app 统计\\n " +
                    " >亚鸿:<font color=\\\"comment\\\"> " + actCountApp + " </font> \\n" +
                    " >阿里:<font color=\\\"comment\\\"> " + aliCountApp + "</font> \\n" +
                    " >共有:<font color=\\\"comment\\\"> " + commonCountApp + " </font> \\n" +
                    "\\n" +
                    " ## 亚鸿阿里汇聚表 短信 统计\\n " +
                    " >亚鸿:<font color=\\\"comment\\\"> " + actCountMsg + " </font> \\n" +
                    " >阿里:<font color=\\\"comment\\\"> " + aliCountMsg + "</font> \\n" +
                    "\" } }";




            try{
                doPost("https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=1ba41b43-6edc-4a85-b505-138e6d64e8b8", param);
            }catch (Exception e) {
                e.printStackTrace();
            }finally {
                restClient.close();
            }





        }


    public static String doPost(String httpUrl, String param) {

        HttpURLConnection connection = null;
        InputStream is = null;
        OutputStream os = null;
        BufferedReader br = null;
        String result = null;
        try {
            URL url = new URL(httpUrl);
            // 通过远程url连接对象打开连接
            connection = (HttpURLConnection) url.openConnection();
            // 设置连接请求方式
            connection.setRequestMethod("POST");
            // 设置连接主机服务器超时时间：15000毫秒
            connection.setConnectTimeout(15000);
            // 设置读取主机服务器返回数据超时时间：60000毫秒
            connection.setReadTimeout(60000);

            // 默认值为：false，当向远程服务器传送数据/写数据时，需要设置为true
            connection.setDoOutput(true);
            // 默认值为：true，当前向远程服务读取数据时，设置为true，该参数可有可无
            connection.setDoInput(true);
            // 设置传入参数的格式:请求参数应该是 name1=value1&name2=value2 的形式。
            connection.setRequestProperty("Content-Type", "application/json");
            // 通过连接对象获取一个输出流
            os = connection.getOutputStream();
            // 通过输出流对象将参数写出去/传输出去,它是通过字节数组写出的
            os.write(param.getBytes());
            // 通过连接对象获取一个输入流，向远程读取
            if (connection.getResponseCode() == 200) {

                is = connection.getInputStream();
                // 对输入流对象进行包装:charset根据工作项目组的要求来设置
                br = new BufferedReader(new InputStreamReader(is, "UTF-8"));

                StringBuffer sbf = new StringBuffer();
                String temp = null;
                // 循环遍历一行一行读取数据
                while ((temp = br.readLine()) != null) {
                    sbf.append(temp);
                    sbf.append("\r\n");
                }
                result = sbf.toString();
            }
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            // 关闭资源
            if (null != br) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (null != os) {
                try {
                    os.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (null != is) {
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            // 断开与远程地址url的连接
            connection.disconnect();
        }
        return result;
    }

}
