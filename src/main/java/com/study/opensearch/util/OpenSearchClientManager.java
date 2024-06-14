package com.study.opensearch.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;

import java.io.IOException;

/**
 * OpenSearch 관련 Util
 */
@Slf4j
public class OpenSearchClientManager {

    /**
     * OpenSearch Client 생성
     *
     * @param hostName
     * @param port
     * @param scheme
     * @return
     */
    public RestHighLevelClient createClient(String hostName, int port, String scheme) {
        try {
            RestClientBuilder builder = RestClient.builder(new HttpHost(hostName, port, scheme));

            return new RestHighLevelClient(builder);
        } catch (Exception e) {
            log.error("Error creating OpenSearch client: {}", e.getMessage());
        }

        return null;
    }

    /**
     * OpenSearch Client 종료
     *
     * @param client
     * @throws IOException
     */
    public void closeClient(RestHighLevelClient client) throws IOException {
        try {
            client.close();
        } catch (IOException e) {
            log.error("Error closing OpenSearch client: {}", e.getMessage());
        }
    }
}
