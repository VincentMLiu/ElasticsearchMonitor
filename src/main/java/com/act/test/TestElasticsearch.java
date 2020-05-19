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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestElasticsearch {




    public static void main(String[] args){


//        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
//        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("admin","admin"));
//
//        RestClientBuilder builder = RestClient.builder(new HttpHost("172.30.132.141", 9211))
//                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
//                    @Override
//                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
//                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
//                    }
//                });
//
//        RestClient restClient = builder.build();
//
//
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




//        Pattern dailyIndexPattern = Pattern.compile("(\\d{4}_(((0(1|3|5|7|8))|(1(0|2)))_(((0[1-9])|([1-2][0-9])|(3[0-1])))?)|(((0(4|6|9))|(11))_(((0[1-9])|([1-2][0-9])|(30)))?)|((02)_(((0[1-9])|(1[0-9])|(2[0-8])))?))" +
//                "|((((\\d{2})((0[48])|([2468][048])|([13579][26]))|(((0[48])|([2468][048])|([3579][26]))00)))_02_29)");


        Pattern dailyIndexPattern =  Pattern.compile("(\\d{4}_\\d{2}_\\d{2})|(\\d{8})");

        String indexName = "test_2000_02_29";



        Matcher matcher = dailyIndexPattern.matcher(indexName);
        System.out.println(matcher.matches());
        System.out.println(matcher.find());



    }

}
