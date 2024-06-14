package com.study.opensearch.index.application;

import com.study.opensearch.entity.CommunityContents;

import java.util.List;
import java.util.Map;

public interface IndexService {

    List<CommunityContents> getCommunityDocuments(Map<String, Object> requestBody);

    Map<String, Object> executeBatchIndex(Map<String, Object> requestBody);

    Map<String, Object> deleteIndex(String indexName);
}
