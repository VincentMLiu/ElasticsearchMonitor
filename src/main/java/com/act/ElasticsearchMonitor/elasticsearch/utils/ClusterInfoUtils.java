package com.act.ElasticsearchMonitor.elasticsearch.utils;

import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ClusterInfoUtils {




    public static List<String> nodesInfoList;

    /**
     * 获取所有的节点名称
     * @param restClient
     * @return
     */
    public static List<String> getNodeInfoList(RestClient restClient){



        if(nodesInfoList != null ){
            return nodesInfoList;
        } else {
            nodesInfoList = new ArrayList<String>();

            try {
                Response responseIdc = restClient.performRequest("GET", "_cat/nodes?");
                InputStreamReader isr = new InputStreamReader(responseIdc.getEntity().getContent());
                BufferedReader bisr = new BufferedReader(isr);
                String lineStr = bisr.readLine();

                while ( (lineStr =  bisr.readLine()) !=null){
                    String newStr = lineStr.replaceAll("\\s+", ",");
                    nodesInfoList.add(newStr);
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
            return nodesInfoList;

        }

    }



    public static int countDataNodesNum(RestClient restClient){
        int dataNodesNum = 0;
        if(nodesInfoList == null){
            nodesInfoList = getNodeInfoList(restClient);
        }

        for(String nodeInfo : nodesInfoList){
            String[] nodeInfoSpli = nodeInfo.split(",");
            if(nodeInfoSpli[7].contains("d")){
                dataNodesNum++;
            }
        }
        return dataNodesNum;

    }




}
