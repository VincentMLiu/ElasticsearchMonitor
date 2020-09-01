package com.act.ElasticsearchMonitor.elasticsearch.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.regex.Pattern;

public class IndicesInfoRequestUtils {


    /**
     * 获取所有的索引名称
     * @param restClient
     * @return
     */
    public static List<String> getIndicesList(RestClient restClient){
        List<String> indicesList = new ArrayList<String>();


        try {
            Response responseIdc = restClient.performRequest("GET", "_cat/indices?v");
            InputStreamReader isr = new InputStreamReader(responseIdc.getEntity().getContent());
            BufferedReader bisr = new BufferedReader(isr);
            String lineStr = bisr.readLine();

            while ( (lineStr =  bisr.readLine()) !=null){
                String newStr = lineStr.replaceAll("\\s+", ",");

                indicesList.add(newStr);

            }


        } catch (IOException e) {
            e.printStackTrace();
        }


        return indicesList;

    }


    public static List<String> getIndicesListInRed(RestClient restClient){
        List<String> indicesList = new ArrayList<String>();
        try {
            Response responseIdc = restClient.performRequest("GET", "_cat/indices?v&health=red");
            InputStreamReader isr = new InputStreamReader(responseIdc.getEntity().getContent());
            BufferedReader bisr = new BufferedReader(isr);
            String lineStr = bisr.readLine();
            while ( (lineStr =  bisr.readLine()) !=null){
                String newStr = lineStr.replaceAll("\\s+", ",");
                indicesList.add(newStr);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return indicesList;
    }


    /*
     * 获取所有的模板
     * @param restClient
     * @return
    * */
    public static Map<Pattern, JSONObject> getTemplateMap(RestClient restClient){

        Map<Pattern,JSONObject> patternMap = new HashMap<Pattern,JSONObject>();

        StringBuffer sb = new StringBuffer();

        try {
            Map<String, String> params = Collections.singletonMap("pretty", "true");
            Response responseTemplate = restClient.performRequest("GET", "_template?", params);

            BufferedReader br = new BufferedReader(new InputStreamReader(responseTemplate.getEntity().getContent()));

            sb = new StringBuffer();

            String data;
            while ((data = br.readLine()) != null) {
                sb.append(data);
            }
            JSONObject job = JSON.parseObject(sb.toString());

            for(Map.Entry<String, Object> entry : job.entrySet()){
                if(entry.getKey().equals("alltemplate")){
                    continue;
                }
                JSONObject templateJson = JSON.parseObject(entry.getValue().toString());
                String templateStr = templateJson.getString("template");
                String newTempL = templateStr.replaceAll("\\*","\\\\w+");
                Pattern columnP = Pattern.compile(newTempL);
                patternMap.put(columnP,templateJson);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return patternMap;

    }


    public static JSONObject getIndexInfo(RestClient restClient, String indexName){

        StringBuffer sb = new StringBuffer();
        JSONObject job;
        try {
            Map<String, String> params = Collections.singletonMap("pretty", "true");
            Response responseTemplate = restClient.performRequest("GET", indexName + "?", params);

            BufferedReader br = new BufferedReader(new InputStreamReader(responseTemplate.getEntity().getContent()));

            sb = new StringBuffer();

            String data;
            while ((data = br.readLine()) != null) {
                sb.append(data);
            }
            job = JSON.parseObject(sb.toString());

            return job.getJSONObject(indexName);
        } catch (IOException e) {
            e.printStackTrace();
            return getIndexInfo(restClient, indexName);
        }



    }


    public static JSONObject deleteIndex(RestClient restClient, String indexName){

        StringBuffer sb = new StringBuffer();
        JSONObject job;
        try {
            Map<String, String> params = Collections.singletonMap("pretty", "true");
            Response responseTemplate = restClient.performRequest("DELETE", indexName + "?", params);

            BufferedReader br = new BufferedReader(new InputStreamReader(responseTemplate.getEntity().getContent()));

            sb = new StringBuffer();

            String data;
            while ((data = br.readLine()) != null) {
                sb.append(data);
            }
            job = JSON.parseObject(sb.toString());

            return job;
        } catch (IOException e) {
            e.printStackTrace();
            return job = JSON.parseObject(sb.toString());
        }



    }

    public static List<String> getShardInfo(RestClient restClient, String indexName){
        List<String> shardsList = new ArrayList<String>();
        try {
            Response responseIdc = restClient.performRequest("GET", "_cat/shards/" + indexName +"?v");
            InputStreamReader isr = new InputStreamReader(responseIdc.getEntity().getContent());
            BufferedReader bisr = new BufferedReader(isr);
            String lineStr = bisr.readLine();
            while ( (lineStr =  bisr.readLine()) !=null){
                String newStr = lineStr.replaceAll("\\s+", ",");
                shardsList.add(newStr);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return shardsList;
    }





}
