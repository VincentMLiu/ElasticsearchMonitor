package com.act.test;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;


public class TestElasticsearch {




    public static void main(String[] args){


        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("test2","s3cr37"));

        RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 19211))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        RestClient restClient = builder.build();
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClient);

        IndexRequest request = new IndexRequest(
                "index2",
                "index",
                "1");
        String jsonString = "{" +
                "\"user\":\"kimchy\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"message\":\"trying out Elasticsearch\"" +
                "}";


        request.source(jsonString, XContentType.JSON);

        try {
            IndexResponse indexResponse = restHighLevelClient.index(request);
            System.out.println(indexResponse.toString());

//            restHighLevelClient.delete(request);

//            String index = indexResponse.getIndex();
//            String type = indexResponse.getType();
//            String id = indexResponse.getId();
//            long version = indexResponse.getVersion();
//            if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
//
//            } else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
//
//            }
//            ReplicationResponse.ShardInfo shardInfo = indexResponse.getShardInfo();
//            if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
//
//            }
//            if (shardInfo.getFailed() > 0) {
//                for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
//                    String reason = failure.reason();
//                }
//            }
        } catch (IOException e) {
            e.printStackTrace();
        }

//        try{
//            Response response = restClient.performRequest("GET", "_cat/indices?v" );
//
//            for(Header hd : response.getHeaders()){
//                System.out.println(hd.getName() + ": " + hd.getValue() );
//            }
//
//            String responseBody = EntityUtils.toString(response.getEntity());
//            System.out.println(responseBody);
//
//            restClient.close();
//        }catch(Exception e){
//            e.printStackTrace();
//        }



    }



    public void testLowLevelClient(){

    }

}
