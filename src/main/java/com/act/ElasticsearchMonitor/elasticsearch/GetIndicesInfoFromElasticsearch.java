package com.act.ElasticsearchMonitor.elasticsearch;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GetIndicesInfoFromElasticsearch {



    public static void main(String[] args){


        RestClient restClient = RestClient.builder(
                new HttpHost("localhost", 19200, "http"))
                .setMaxRetryTimeoutMillis(10000)
                .build();

        try{
            File newFile = new File("D:\\Desktop\\es集群信息.csv");
            OutputStream os = new FileOutputStream(newFile);

            StringBuffer sb = new StringBuffer();

            Map<String, String> params = Collections.singletonMap("pretty", "true");

            Response responseTemplate = restClient.performRequest("GET", "_template?", params);

            BufferedReader br = new BufferedReader(new InputStreamReader(responseTemplate.getEntity().getContent()));

            sb = new StringBuffer();

            String data;
            while ((data = br.readLine()) != null) {
                sb.append(data);
            }
            JSONObject job = JSON.parseObject(sb.toString());

            List<Pattern> patternList = new ArrayList<Pattern>();

            for(Map.Entry<String, Object> entry : job.entrySet()){
                JSONObject templateJson = JSON.parseObject(entry.getValue().toString());
                String templateStr = templateJson.getString("template");
                String newTempL = templateStr.replaceAll("\\*","\\\\w+");
                Pattern columnP = Pattern.compile(newTempL);
                patternList.add(columnP);
            }



            Response responseIdc = restClient.performRequest("GET", "_cat/indices?v", params);

            InputStreamReader isr = new InputStreamReader(responseIdc.getEntity().getContent());
            BufferedReader bisr = new BufferedReader(isr);
            String lineStr = bisr.readLine();
            sb = new StringBuffer();
            while ( (lineStr =  bisr.readLine()) !=null){

                String newStr = lineStr.replaceAll("\\s+", ",");
                System.out.println(newStr);
                sb.append(newStr);
                String[] lineSpli = newStr.split(",");

                String template = ",\n";
                if (lineSpli.length > 2){
                    for(Pattern pt : patternList){
                        Matcher mt =  pt.matcher(lineSpli[2]);
                        if(mt.matches()){
                            template = "," + pt.pattern() + "\n" ;

                            break;
                        }
                    }
                }
                sb.append(template);

            }
            os.write(sb.toString().getBytes());

            os.flush();
            os.close();
            restClient.close();
        }catch (IOException ioE) {
            ioE.printStackTrace();
        }


    }





}
