package com.act.ElasticsearchMonitor.elasticsearch;

import com.act.ElasticsearchMonitor.elasticsearch.utils.ConfigerationUtils;
import com.act.ElasticsearchMonitor.elasticsearch.utils.IndicesInfoRequestUtils;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

public class IndicesShardReroute {


    private static SimpleDateFormat yyyyMMdd = new SimpleDateFormat("yyyy-MM-dd");

    private static RestClient restClient;

    private static int dataNodeNum = 0;


    private static Date today = new Date();


        public static void main(String[] args) throws IOException {

            //ES连接信息
            ConfigerationUtils.init("indicesDelete.properties");
            String esHost = ConfigerationUtils.get("esHost", "localhost");
            int esPort = Integer.parseInt(ConfigerationUtils.get("esPort", "19200"));
//            String esHost = "172.16.16.41";
//            int esPort = 19200;
            restClient = RestClient.builder(
                    new HttpHost(esHost, esPort, "http"))
                    .setMaxRetryTimeoutMillis(1000000)
                    .build();

            String nodeListStr = "node-172.16.16.44,node-172.16.16.42,node-172.16.16.43,node-172.16.16.33,node-172.16.16.58,node-172.16.16.38,node-172.16.16.40,node-172.16.16.41,node-172.16.16.34,node-172.16.16.39,node-172.16.16.56,node-172.16.16.35,node-172.16.16.37";

            List<String> nodeList = Arrays.asList(nodeListStr.split(","));

            //红索引
            List<String> redIndicesInfoList = IndicesInfoRequestUtils.getIndicesListInRed(restClient);
            JSONArray commands = new JSONArray();


            int index = 0;
            for(String indexInfo: redIndicesInfoList){
                String[] indexInfoSpli = indexInfo.split(",");
                String indexName = indexInfoSpli[2];
                List<String> redIndicesShardsInfoList = IndicesInfoRequestUtils.getShardInfo(restClient, indexName);
                List<String[]> unAssignList = new ArrayList<>();
                List<String> existsNodeList = new ArrayList<>();
                for(String shardInfo : redIndicesShardsInfoList){
                    String[] shardInfoSpli = shardInfo.split(",");
                    String state = shardInfoSpli[3];

                    int shardIndex = Integer.parseInt(shardInfoSpli[1]);

                    if(state.equals("UNASSIGNED")){
                        index++;
                        System.out.println(index + ":" + shardInfo);
                        //拼装
                        JSONObject allocate_empty_primary = new JSONObject();
                        allocate_empty_primary.put("index", indexName);
                        allocate_empty_primary.put("shard", shardIndex);
                        allocate_empty_primary.put("node", nodeList.get(index%nodeList.size()));
                        allocate_empty_primary.put("accept_data_loss", true);
                        JSONObject outObj = new JSONObject();
                        outObj.put("allocate_empty_primary", allocate_empty_primary);
                        commands.add(outObj);
                        System.out.println(allocate_empty_primary.toJSONString());

                    }
                }


            }


            System.out.println(commands.toJSONString());

            Map<String,String> param = new HashMap<>();

            param.put("pretty", "true");
            param.put("timeout", "5m");

            HttpEntity he = new NStringEntity(
                    "{ \"commands\" :"+ commands.toJSONString() + "}", ContentType.APPLICATION_JSON
            );



            Response rp = restClient.performRequest("POST", "/_cluster/reroute?", param, he);
            BufferedReader br = new BufferedReader(new InputStreamReader(rp.getEntity().getContent()));

            StringBuffer sb = new StringBuffer();

            String data;
            while ((data = br.readLine()) != null) {
                sb.append(data);
            }

            System.out.println(sb.toString());
        }




}
