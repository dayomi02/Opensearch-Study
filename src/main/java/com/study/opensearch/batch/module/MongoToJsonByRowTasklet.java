package com.study.opensearch.batch.module;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.study.opensearch.util.Constants;
import com.study.opensearch.util.UUIDGenerator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileWriter;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class MongoToJsonByRowTasklet implements Tasklet {

    private final MongoTemplate mongoTemplate;

    @Value("${index.docs.path.source}")
    private String indexDocsPath;

    @Value("${index.docs.name}")
    private String indexDocsName;

    @Value("${index.docs.collection}")
    private String indexDocsCollection;

    @Value("${index.docs.keyCode}")
    private String indexDocsKeyCode;

    /**
     * MongoDB 데이터 1건 씩 Json 파일로 저장
     *
     * @return result
     * @throws Exception error
     */
    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        JobParameters jobParameters = chunkContext.getStepContext().getStepExecution().getJobParameters();
        String currentDate = jobParameters.getString("currentDate");

        Query query = new Query();
        query.addCriteria(Criteria.where(Constants.DOC_KEY_CODE).is(indexDocsKeyCode));
        query.with(Sort.by(Sort.Direction.ASC, "publish_date"));

        List<Document> documents = mongoTemplate.find(query, Document.class, indexDocsCollection);

        ObjectMapper objectMapper = new ObjectMapper();
        for (Document document : documents) {
//            String id = document.get("_id").toString();
//            String fileName = indexDocsPath + "byRow\\" + indexDocsName + "_" + currentDate + "_" + id + ".json";
            String fileName = indexDocsPath + "byRow\\" + indexDocsName + "_" + UUIDGenerator.generateUUID() + ".json";
            File file = new FileSystemResource(fileName).getFile();

            try (FileWriter fileWriter = new FileWriter(file)) {
                objectMapper.writeValue(fileWriter, document);
            } catch (Exception e) {
                log.error("Error Occurred. {}", e.getMessage());
            }

            log.info("::: Write Json File. fileName = {} :::", fileName);
        }

        return RepeatStatus.FINISHED;
    }
}
