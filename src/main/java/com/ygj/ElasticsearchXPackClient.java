package com.ygj;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class ElasticsearchXPackClient {

    private static final Logger log = LoggerFactory.getLogger(ElasticsearchXPackClient.class);

    protected RestHighLevelClient client;

    private String hello;

    public void init() throws Exception {
        RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost("es-dev01.yingzi.com", 9200, "http"),
                new HttpHost("es-dev02.yingzi.com", 9200, "http"),
                new HttpHost("es-dev03.yingzi.com", 9200, "http"));
        // 下面步骤目的是设置x-pack认证的用户名和密码（restFulAPI）
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("elastic", "elastic"));
        restClientBuilder.setHttpClientConfigCallback((httpAsyncClientBuilder) -> {
            return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
        });
        client = new RestHighLevelClient(restClientBuilder);
    }

    public static void main(String[] args) throws Exception {
        ElasticsearchXPackClient elasticsearchXPackClient = new ElasticsearchXPackClient();
        elasticsearchXPackClient.init();
        elasticsearchXPackClient.searchInfo();
        String jsonString = "{" +
                "\"user\":\"kimchy\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"message\":\"trying out Elasticsearch\"" +
                "}";
        elasticsearchXPackClient.insertData("bak_test", jsonString);
        elasticsearchXPackClient.close();
    }

    public void searchInfo() throws IOException {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.termQuery("rect_id", 325600167));
        sourceBuilder.from(0);
        sourceBuilder.size(5);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("iot_data");
        searchRequest.source(sourceBuilder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(response.getHits().getTotalHits());
    }

    public void insertData(String indexName, String jsonString) throws IOException {
        IndexRequest request = new IndexRequest(indexName,"doc");
        request.source(jsonString, XContentType.JSON);
        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
        log.info("es result:{1}", indexResponse.toString());
    }

    public void close() throws Exception {
        if (client != null) {
            client.close();
        }
    }

}
