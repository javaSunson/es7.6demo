package com.hmwl.myes.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author martin
 * @version 1.0
 * @name: ElasticConfig
 * @date: 2021/2/1 9:08
 * @description
 * @comepony 北京百途
 **/
@Configuration
public class ElasticConfig {
    @Bean
    public RestHighLevelClient restHighLevelClient(){
        RestHighLevelClient client = new RestHighLevelClient(
                // 构建者支持 单机 和 集群配置。
                RestClient.builder(
                        new HttpHost("localhost", 19201, "http")));

            return client;
        }

}
