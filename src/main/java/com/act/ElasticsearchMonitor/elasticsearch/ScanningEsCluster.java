package com.act.ElasticsearchMonitor.elasticsearch;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

public class ScanningEsCluster {

    RestClient restClient = RestClient.builder(
            new HttpHost("localhost", 19200, "http"))
            .setMaxRetryTimeoutMillis(10000)
            .build();

    public static void main(String[] args){



    }



}
