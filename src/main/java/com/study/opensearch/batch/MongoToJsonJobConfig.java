package com.study.opensearch.batch;

import com.study.opensearch.batch.module.ExecutionTimeLogger;
import com.study.opensearch.batch.module.JsonDocFileItemWriter;
import com.study.opensearch.batch.module.MongoItemReader;
import com.study.opensearch.entity.CommunityContents;
import com.study.opensearch.util.Constants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.MongoCursorItemReader;
import org.springframework.batch.item.data.builder.MongoCursorItemReaderBuilder;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.json.JsonFileItemWriter;
import org.springframework.batch.item.json.builder.JsonFileItemWriterBuilder;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * MongoDB 데이터 Json 파일로 저장
 */
@Slf4j
@Configuration
public class MongoToJsonJobConfig {

    private final JobRepository jobRepository;

    private final PlatformTransactionManager transactionManager;

    private final MongoTemplate mongoTemplate;

    private final Step registerLogStep;

    private final MongoItemReader mongoItemReader;

    private final JsonDocFileItemWriter jsonDocFileItemWriter;

    private final ExecutionTimeLogger executionTimeLogger;

    @Value("${index.docs.path.source}")
    private String indexDocsPath;

    @Value("${index.docs.name}")
    private String indexDocsName;

    @Value("${index.docs.keyCode}")
    private String indexDocsKeyCode;

    @Value("${index.docs.collection}")
    private String indexDocsCollection;

    public MongoToJsonJobConfig(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                                MongoTemplate mongoTemplate,
                                @Qualifier("registerLogStep") Step registerLogStep,
                                @Qualifier("mongoItemReader") MongoItemReader mongoItemReader,
                                @Qualifier("jsonDocFileItemWriter") JsonDocFileItemWriter jsonDocFileItemWriter,
                                @Qualifier("executionTimeLogger") ExecutionTimeLogger executionTimeLogger) {
        this.jobRepository = jobRepository;
        this.transactionManager = transactionManager;
        this.mongoTemplate = mongoTemplate;
        this.registerLogStep = registerLogStep;
        this.mongoItemReader = mongoItemReader;
        this.jsonDocFileItemWriter = jsonDocFileItemWriter;
        this.executionTimeLogger = executionTimeLogger;
    }

    @Bean
    public Job mongoToJsonJob(@Qualifier("mongoToJsonStep") Step mongoToJsonStep) {
        return new JobBuilder("mongoToJsonJob", jobRepository)
                .start(mongoToJsonStep)
                .next(registerLogStep)
                .listener(executionTimeLogger)
                .build();
    }

    @Bean
    public Step mongoToJsonStep(@Qualifier("compositeItemWriter") CompositeItemWriter<CommunityContents> compositeItemWriter) {
        // MongoTemplate 사용
        return new StepBuilder("mongoToJsonStep", jobRepository)
                .<CommunityContents, CommunityContents>chunk(1000, transactionManager)
                .reader(indexDocsItemReader())
                .writer(compositeItemWriter)
                .listener(executionTimeLogger)
                .build();

        // MongoClient 사용
//        return new StepBuilder("mongoToJsonStep", jobRepository)
//                .<Document, Document>chunk(1000, transactionManager)
//                .reader(mongoItemReader)
//                .writer(jsonDocFileItemWriter)
//                .listener(executionTimeLogger)
//                .build();
    }

    /**
     * mongo data 조회
     * @return MongoCursorITemReader
     */
    @Bean
    public MongoCursorItemReader<CommunityContents> indexDocsItemReader() {
        return new MongoCursorItemReaderBuilder<CommunityContents>()
                .name("indexDocsItemReader")
                .template(mongoTemplate)
                .targetType(CommunityContents.class)
                .query(new Query().addCriteria(Criteria.where(Constants.DOC_KEY_CODE).is(indexDocsKeyCode)
                        .and(Constants.DOC_IS_INDEXED).is(Constants.FLAG_N)
                        ))
                .sorts(Collections.singletonMap("publish_date", Sort.Direction.ASC))
                .build();
    }

    /**
     * mongo data json 파일로 쓰기
     * @param currentDate 실행 날짜
     * @return JsonFileItemWriter
     */
    @Bean
    @StepScope
    public JsonFileItemWriter<CommunityContents> jsonFileItemWriter(@Value("#{jobParameters['currentDate']}") String currentDate) {
        String filePath = indexDocsPath + indexDocsName + "_" + currentDate + ".json";

        return new JsonFileItemWriterBuilder<CommunityContents>()
                .jsonObjectMarshaller(new JacksonJsonObjectMarshaller<>())
                .resource(new FileSystemResource(filePath))
                .name("jsonFileItemWriter")
                .build();
    }

    /**
     * mongo data write
     * @return ItemWriter
     */
    @Bean
    public ItemWriter<CommunityContents> mongoItemWriter(){
        return items -> {
            BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.ORDERED, indexDocsCollection);

            for (CommunityContents item : items){
                Query query = new Query(Criteria.where(Constants.DOC_ID).is(item.get_id()));
                Update update = new Update().set(Constants.DOC_IS_INDEXED, Constants.FLAG_Y);
                bulkOperations.updateOne(query, update);
            }

            bulkOperations.execute();
        };
    }

//    /**
//     * json 파일로 만들어진 data update
//     * @return ItemProcessor
//     */
//    @Bean
//    public ItemProcessor<CommunityContents, CommunityContents> indexDocsItemProcessor(){
//        return item -> {
//            item.setIsIndexed(Constants.FLAG_Y);
//
//            return item;
//        };
//    }

    /**
     * 여러 개의 writer 사용을 위한 CompositeItemWriter
     * @param jsonFileItemWriter jsonFileItemWriter
     * @param mongoItemWriter mongoItemWriter
     * @return CompositeItemWriter
     */
    @Bean
    public CompositeItemWriter<CommunityContents> compositeItemWriter(@Qualifier("jsonFileItemWriter") JsonFileItemWriter<CommunityContents> jsonFileItemWriter,
                                                                      @Qualifier("mongoItemWriter") ItemWriter<CommunityContents> mongoItemWriter) {
        List<ItemWriter<? super CommunityContents>> writers = new ArrayList<>();
        writers.add(jsonFileItemWriter);
        writers.add(mongoItemWriter);

        CompositeItemWriter<CommunityContents> compositeItemWriter = new CompositeItemWriter<>();
        compositeItemWriter.setDelegates(writers);

        return compositeItemWriter;
    }
}
