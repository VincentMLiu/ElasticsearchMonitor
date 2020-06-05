package com.act.test;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;


public class TestElasticsearch {




    public static void main(String[] args){


        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("test","test"));

        RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 19211))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        RestClient restClient = builder.build();


        try{
            Response response = restClient.performRequest("GET", "_cat/indices?v" );

            for(Header hd : response.getHeaders()){
                System.out.println(hd.getName() + ": " + hd.getValue() );
            }

            String responseBody = EntityUtils.toString(response.getEntity());
            System.out.println(responseBody);

            restClient.close();
        }catch(Exception e){
            e.printStackTrace();
        }



    }



    public void testLowLevelClient(){

    }

}
