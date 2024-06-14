package com.study.opensearch.batch.module;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.study.opensearch.entity.CommunityContents;
import com.study.opensearch.util.Constants;
import com.study.opensearch.util.UUIDGenerator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.json.simple.JSONObject;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class MongoToBulkJsonByRowTasklet implements Tasklet {

    private final MongoTemplate mongoTemplate;

    @Value("${index.docs.path.source}")
    private String indexDocsPath;

    @Value("${index.docs.name}")
    private String indexDocsName;

    @Value("${index.docs.collection}")
    private String indexDocsCollection;

    @Value("${index.docs.keyCode}")
    private String indexDocsKeyCode;

    @Value("${index.docs.indexName}")
    private String indexDocsIndexNme;

    private int index_id = 1;
    /**
     * MongoDB 데이터 1건 씩 Bulk Json 파일로 저장
     *
     * @return result
     * @throws Exception error
     */
    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        JobParameters jobParameters = chunkContext.getStepContext().getStepExecution().getJobParameters();
        String currentDate = jobParameters.getString("currentDate");

        Query query = new Query();
        query.addCriteria(Criteria.where(Constants.DOC_KEY_CODE).is(indexDocsKeyCode)
                .and(Constants.DOC_IS_INDEXED).is(Constants.FLAG_N)
        );
        query.with(Sort.by(Sort.Direction.ASC, "publish_date"));

        List<CommunityContents> documents = mongoTemplate.find(query, CommunityContents.class, indexDocsCollection);

        ObjectMapper objectMapper = new ObjectMapper();
        for (CommunityContents item : documents) {

            JSONObject indexJson = new JSONObject();
            JSONObject indexInfoJson = new JSONObject();
            indexInfoJson.put(Constants.INDEX__INDEX, indexDocsIndexNme);
            indexInfoJson.put(Constants.INDEX__ID, index_id++);
            indexJson.put(Constants.INDEX_INDEX, indexInfoJson);

            JSONObject fieldJson = new JSONObject();
            fieldJson.put(Constants.INDEX_KEY_CODE, item.get_id());
//            fieldJson.put(Constants.INDEX_URL, item.get_id());
            fieldJson.put(Constants.INDEX_CATEGORY, item.getCategory());
            fieldJson.put(Constants.INDEX_CONTENT, item.getContent());
            fieldJson.put(Constants.INDEX_DESCRIPTION, item.getDescription());
            fieldJson.put(Constants.INDEX_NAME, item.getName());
            fieldJson.put(Constants.INDEX_PUBLISH_DATE, item.getPublishDate());
            fieldJson.put(Constants.INDEX_TITLE, item.getTitle());
            fieldJson.put(Constants.INDEX_QUERY, item.getQuery());

            String[] bulkItem = {item.get_id(), indexJson.toString(), fieldJson.toString()};

            BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.ORDERED, indexDocsCollection);
            String filePath = indexDocsPath + indexDocsName + "_" + (index_id - 1) + ".json";

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, true))) {
                writer.write(bulkItem[1]);
                writer.newLine();
                writer.write(bulkItem[2]);
                writer.newLine();

                Query updateQuery = new Query(Criteria.where(Constants.DOC_ID).is(bulkItem[0]));
                Update update = new Update().set(Constants.DOC_IS_INDEXED, Constants.FLAG_Y);
                bulkOperations.updateOne(updateQuery, update);

            } catch (IOException e) {
                log.error("Create bulk json file failed... message: {}", e.getMessage());
            }

            bulkOperations.execute();

            log.info("::: Write Json File. fileName = {} :::", filePath);
        }

        return RepeatStatus.FINISHED;
    }
}
