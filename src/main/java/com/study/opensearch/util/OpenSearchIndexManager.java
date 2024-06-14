package com.study.opensearch.util;

import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.CreateIndexResponse;
import org.opensearch.client.indices.GetIndexRequest;

import java.io.IOException;
import java.util.Map;

/**
 * OpenSearch 관련 Util
 */
@Slf4j
public class OpenSearchIndexManager {

    /**
     * index 존재 여부 확인
     *
     * @param indexName
     * @param client
     * @return
     */
    public boolean isExistIndex(String indexName, RestHighLevelClient client) {
        GetIndexRequest request = new GetIndexRequest(indexName);

        try {
            return client.indices().exists(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("Error check index existence: {}", e.getMessage());
            return false;
        }
    }

    /**
     * index 삭제
     *
     * @param indexName
     * @param client
     * @return
     */
    public boolean deleteIndex(String indexName, RestHighLevelClient client) {
        DeleteIndexRequest request = new DeleteIndexRequest(indexName);

        try {
            AcknowledgedResponse response = client.indices().delete(request, RequestOptions.DEFAULT);
            if (response.isAcknowledged()) {
                log.info("Index deletion successfully: index={}", indexName);

                return true;
            } else {
                log.warn("Index deletion failed: index={}", indexName);

                return false;
            }
        } catch (IOException e) {
            log.error("Error deleting index: {}", e.getMessage());

            return false;
        }
    }

    /**
     * index 생성
     *
     * @param indexName
     * @param client
     * @return
     */
    public boolean createIndex(String indexName, RestHighLevelClient client) {
        CreateIndexRequest request = new CreateIndexRequest(indexName);

        try {
            CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
            if (response.isAcknowledged()) {
                log.info("Index creation successfully: index={}", indexName);

                return true;
            } else {
                log.warn("Index creation failed: index={}", indexName);

                return false;
            }
        } catch (IOException e) {
            log.error("Error creating index: {}", e.getMessage());

            return false;
        }
    }

    /**
     * indexing
     *
     * @param indexName
     * @param documentId
     * @param document
     * @param client
     * @return
     */
    public boolean indexDocument(String indexName, String documentId, Map<String, Object> document, RestHighLevelClient client) {
//        Map<String, Object> resultMap = new HashMap<>();
        IndexRequest request = new IndexRequest(indexName).id(documentId).source(document);//, XContentType.JSON);

        try {
            client.index(request, RequestOptions.DEFAULT);
            log.info("Index successfully: index={}, docId={}", indexName, documentId);

            return true;
        } catch (IOException e) {
            log.error("Error indexing index: {}", e.getMessage());

            return false;
        }
    }
}
