package com.study.opensearch.batch;

import com.study.opensearch.batch.module.ExecutionTimeLogger;
import com.study.opensearch.entity.CommunityContents;
import com.study.opensearch.util.Constants;
import lombok.extern.slf4j.Slf4j;
import org.json.simple.JSONObject;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.MongoCursorItemReader;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.transaction.PlatformTransactionManager;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;


@Slf4j
@Configuration
public class MongoToBulkJsonJobConfig {

    private final JobRepository jobRepository;

    private final PlatformTransactionManager transactionManager;

    private final MongoTemplate mongoTemplate;

    private final ExecutionTimeLogger executionTimeLogger;

    private final MongoCursorItemReader<CommunityContents> indexDocsItemReader;

    @Value("${index.docs.path.source}")
    private String indexDocsPath;

    @Value("${index.docs.name}")
    private String indexDocsName;

    @Value("${index.docs.indexName}")
    private String indexDocsIndexNme;

    @Value("${index.docs.collection}")
    private String indexDocsCollection;

    private int index_id = 1;

    public MongoToBulkJsonJobConfig(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                                    MongoTemplate mongoTemplate,
                                    @Qualifier("executionTimeLogger") ExecutionTimeLogger executionTimeLogger,
                                    @Qualifier("indexDocsItemReader") MongoCursorItemReader<CommunityContents> indexDocsItemReader){
        this.jobRepository = jobRepository;
        this.transactionManager = transactionManager;
        this.mongoTemplate = mongoTemplate;
        this.executionTimeLogger = executionTimeLogger;
        this.indexDocsItemReader = indexDocsItemReader;
    }

    @Bean
    public Job mongoToBulkJsonJob(){
        return new JobBuilder("mongoToBulkJsonJob", jobRepository)
                .start(mongoToBulkJsonStep())
                .listener(executionTimeLogger)
                .build();
    }

    @Bean
    public Step mongoToBulkJsonStep(){
        return new StepBuilder("mongoToBulkJsonStep", jobRepository)
                .<CommunityContents, String[]>chunk(1000, transactionManager)
//                .<CommunityContents, String[]>chunk(1, transactionManager)
                .reader(indexDocsItemReader)
                .processor(indexBulkItemProcessor())
                .writer(bulkJsonFileItemWriter(null))
                .listener(executionTimeLogger)
                .build();
    }

    @Bean
    public ItemProcessor<CommunityContents, String[]> indexBulkItemProcessor(){
        return item -> {

            JSONObject indexJson = new JSONObject();
            JSONObject indexInfoJson = new JSONObject();
            indexInfoJson.put(Constants.INDEX__INDEX, indexDocsIndexNme);
            indexInfoJson.put(Constants.INDEX__ID, index_id++);
            indexJson.put(Constants.INDEX_INDEX, indexInfoJson);

            JSONObject fieldJson = new JSONObject();
            fieldJson.put(Constants.INDEX_KEY_CODE, item.getKeyCode());
            fieldJson.put(Constants.INDEX_URL, item.get_id());
            fieldJson.put(Constants.INDEX_CATEGORY, item.getCategory());
            fieldJson.put(Constants.INDEX_CONTENT, item.getContent());
            fieldJson.put(Constants.INDEX_DESCRIPTION, item.getDescription());
            fieldJson.put(Constants.INDEX_NAME, item.getName());
            fieldJson.put(Constants.INDEX_PUBLISH_DATE, item.getPublishDate());
            fieldJson.put(Constants.INDEX_TITLE, item.getTitle());
            fieldJson.put(Constants.INDEX_QUERY, item.getQuery());

            String[] bulkItem = {item.get_id(), indexJson.toString(), fieldJson.toString()};

            return bulkItem;
//            return new String[]{item.get_id(), indexJson.toString(), fieldJson.toString()};
        };
    }

    @Bean
    @StepScope
    public ItemWriter<String[]> bulkJsonFileItemWriter(@Value("#{jobParameters['currentDate']}") String currentDate) {
        String filePath = indexDocsPath + indexDocsName + "_" + currentDate + ".json";

        return new ItemWriter<String[]>() {
            @Override
            public void write(Chunk<? extends String[]> chunk) throws Exception {
                BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.ORDERED, indexDocsCollection);
//                String filePath = indexDocsPath + indexDocsName + "_" + (index_id - 1) + ".json";

                try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, true))){
                    for(String[] item : chunk){
                        writer.write(item[1]);
                        writer.newLine();
                        writer.write(item[2]);
                        writer.newLine();

                        Query query = new Query(Criteria.where(Constants.DOC_ID).is(item[0]));
                        Update update = new Update().set(Constants.DOC_IS_INDEXED, Constants.FLAG_Y);
                        bulkOperations.updateOne(query, update);
                    }
                }catch (IOException e){
                    log.error("Create bulk json file failed... message: {}", e.getMessage());
                }

                bulkOperations.execute();
            }
        };
    }
}
