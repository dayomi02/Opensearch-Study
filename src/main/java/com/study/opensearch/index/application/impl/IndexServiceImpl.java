package com.study.opensearch.index.application.impl;

import com.study.opensearch.entity.CommunityContents;
import com.study.opensearch.index.application.IndexService;
import com.study.opensearch.util.Constants;
import com.study.opensearch.util.OpenSearchClientManager;
import com.study.opensearch.util.OpenSearchIndexManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RequiredArgsConstructor
@Service
@Slf4j
public class IndexServiceImpl implements IndexService {

    @Autowired
    private MongoTemplate mongoTemplate;

    @Value("${opensearch.ip}")
    private String opensearchIp;

    @Value("${opensearch.port}")
    private int opensearchPort;

    OpenSearchIndexManager openSearchIndexManager = new OpenSearchIndexManager();

    RestHighLevelClient client = null;

    /**
     * community documents 조회
     */
    public List<CommunityContents> getCommunityDocuments(Map<String, Object> request) {
        String keyCode = (String) request.get(Constants.REQ_KEY_CODE);
        Query query = new Query().addCriteria(Criteria.where(Constants.DOC_KEY_CODE).is(keyCode));

        return mongoTemplate.find(query, CommunityContents.class);
    }

    /**
     * batch index
     */
    public Map<String, Object> executeBatchIndex(Map<String, Object> request) {
        Map<String, Object> resultMap = new HashMap<>();
        Map<String, Object> communityRequest = new HashMap<>() {{
            put(Constants.REQ_KEY_CODE, request.get(Constants.REQ_KEY_CODE));
        }};
        String indexName = (String) request.get(Constants.REQ_INDEX_NAME);
        int documentCount = 0;

        // OpenSearch Connection
        OpenSearchClientManager clientManager = new OpenSearchClientManager();
        client = clientManager.createClient(opensearchIp, opensearchPort, Constants.HTTP);

        if (openSearchIndexManager.isExistIndex(indexName, client))
            openSearchIndexManager.deleteIndex(indexName, client);

        openSearchIndexManager.createIndex(indexName, client);

        List<CommunityContents> documents = getCommunityDocuments(communityRequest);

        for (CommunityContents document : documents) {
            Map<String, Object> indexData = new HashMap<>();

            indexData.put(Constants.INDEX_TITLE, document.getTitle());
            indexData.put(Constants.INDEX_NAME, document.getName());
            indexData.put(Constants.INDEX_CONTENT, document.getContent());

            boolean indexResult = openSearchIndexManager.indexDocument(indexName, document.get_id(), indexData, client);

            if (indexResult)
                documentCount++;
        }

        resultMap.put(Constants.RES_INDEX_NAME, indexName);
        resultMap.put(Constants.RES_DOCUMENT_COUNT, documentCount);

        return resultMap;
    }

    /**
     * delete index
     * @param indexName 삭제할 색인 명
     * @return result
     */
    public Map<String, Object> deleteIndex(String indexName) {
        Map<String, Object> resultMap = new HashMap<>();
        String message = "";

        try{
            OpenSearchClientManager clientManager = new OpenSearchClientManager();
            client = clientManager.createClient(opensearchIp, opensearchPort, Constants.HTTP);

            if (openSearchIndexManager.isExistIndex(indexName, client))
                openSearchIndexManager.deleteIndex(indexName, client);

            clientManager.closeClient(client);
        }catch(Exception e){
            log.error("Error Occurred... message: {}", e.getMessage());
            message = e.getMessage();
        }

        message = "Index Deletion Successful. indexName: [" + indexName + "]";
        resultMap.put(Constants.RES_MESSAGE, message);

        return resultMap;
    }
}
