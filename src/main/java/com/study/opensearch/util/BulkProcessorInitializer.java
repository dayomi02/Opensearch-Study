package com.study.opensearch.util;

import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.bulk.BackoffPolicy;
import org.opensearch.action.bulk.BulkProcessor;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.client.OpenSearchClient;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.unit.ByteSizeUnit;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.common.unit.TimeValue;

@Slf4j
public class BulkProcessorInitializer {

    public static BulkProcessor initializeBulkProcessor(RestHighLevelClient client, int bulkActions) {

        BulkProcessor bulkProcessor = BulkProcessor
//                .builder(
//                        (request, bulkListener) -> client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener))
//                .setBulkActions(bulkActions)
//                .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))
//                .setFlushInterval(TimeValue.timeValueSeconds(5))
//                .setConcurrentRequests(1)
//                .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                .builder((request, bulkListener) -> client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
                        new BulkProcessor.Listener() {
                            @Override
                            public void beforeBulk(long executionId, BulkRequest request) {
                                // Bulk 요청 전 호출
                            }

                            @Override
                            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                                // Bulk 요청 성공 후 호출
                            }

                            @Override
                            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                                // Bulk 요청 실패 후 호출
                            }
                        })
                .setBulkActions(bulkActions)
                .setConcurrentRequests(0)
                .setFlushInterval(TimeValue.timeValueSeconds(10L))
                .build();

        return bulkProcessor;
    }
}
