package com.study.opensearch.batch;

import com.study.opensearch.batch.module.ExecutionTimeLogger;
import com.study.opensearch.entity.CommunityContents;
import com.study.opensearch.util.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.FileEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.opensearch.action.bulk.BulkProcessor;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.xcontent.XContentType;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * open search 의 BulkProcessor 를 활용한 색인 작업
 * json to index
 */
@Slf4j
@Configuration
public class OpenSearchBatchJobConfig {

    private final JobRepository jobRepository;

    private final PlatformTransactionManager transactionManager;

    private final ExecutionTimeLogger executionTimeLogger;

    OpenSearchIndexManager openSearchIndexManager = new OpenSearchIndexManager();

    RestHighLevelClient client = null;

    @Value("${opensearch.ip}")
    private String opensearchIp;

    @Value("${opensearch.port}")
    private int opensearchPort;

    @Value("${index.docs.indexName}")
    private String indexDocsIndexNme;

    @Value("${index.docs.path.backup}")
    private String indexDocsPathBackup;

    @Value("${index.docs.path.separator}")
    private String indexDocsPathSeparator;

//    private int testCount = 1;

    public OpenSearchBatchJobConfig(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                                    @Qualifier("executionTimeLogger") ExecutionTimeLogger executionTimeLogger) {
        this.jobRepository = jobRepository;
        this.transactionManager = transactionManager;
        this.executionTimeLogger = executionTimeLogger;
    }

    /**
     * open search batch 작업
     * @return job
     */
    @Bean
    public Job openSearchBatchJob() {
        log.info("::: [{}] method Start :::", Thread.currentThread().getStackTrace()[1].getMethodName());
        return new JobBuilder("openSearchBatchJob", jobRepository)
                .start(jsonToOpenSearchStep())
                .next(fileBackupStep())
                .listener(executionTimeLogger)
                .build();
    }

    /**
     * json 색인
     * @return step
     */
    @Bean
    public Step jsonToOpenSearchStep() {
        log.info("::: [{}] method Start :::", Thread.currentThread().getStackTrace()[1].getMethodName());
        // BulkProcessor - simple json
//        return new StepBuilder("jsonToOpenSearchStep", jobRepository)
//                .<CommunityContents, Map<String, Object>>chunk(1000, transactionManager)
//                .reader(jsonItemReader(null))
//                .processor(openSearchItemProcessor())
//                .writer(openSearchItemWriter(null))
//                .listener(executionTimeLogger)
//                .build();

        // BulkRequest - Bulk json
        return new StepBuilder("jsonToOpenSearchStep", jobRepository)
                .tasklet(openSearchBulkIndexTasklet(null), transactionManager)
                .build();
    }

    @Bean
    @StepScope
    public Tasklet openSearchBulkIndexTasklet(@Value("#{jobParameters['jsonFilePath']}") String jsonFilePath) {
        return (contribution, chunkContext) -> {

                String apiUrl = "http://" +opensearchIp + ":" + opensearchPort + "/_bulk";

                try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
                    HttpPost postRequest = new HttpPost(apiUrl);
                    postRequest.addHeader("Content-Type", "application/json");
                    postRequest.addHeader("Accept", "*/*");
//                    postRequest.addHeader("Connection", "keep-alive");

                    HttpEntity entity = new FileEntity(Paths.get(jsonFilePath).toFile());
                    postRequest.setEntity(entity);

                    HttpResponse response = httpClient.execute(postRequest);
                    HttpEntity responseEntity = response.getEntity();

                    if (responseEntity != null) {
                        String responseString = EntityUtils.toString(responseEntity);
                        System.out.println("Response: " + responseString);
                    }
                } catch (IOException e) {
                    log.error("Index failed. message: {}", e.getMessage());
                }

            return RepeatStatus.FINISHED;
        };
    }

    /**
     * 색인 한 json file 백업
     * @return step
     */
    @Bean
    public Step fileBackupStep() {
        log.info("::: [{}] method Start :::", Thread.currentThread().getStackTrace()[1].getMethodName());
        return new StepBuilder("fileBackupStep", jobRepository)
                .tasklet(fileBackupTasklet(null), transactionManager)
                .build();
    }

    /**
     * json file data read
     * @param jsonFilePath 색인 파일 위치
     * @return JsonItemReader
     */
    @Bean
    @StepScope
    public JsonItemReader<CommunityContents> jsonItemReader(@Value("#{jobParameters['jsonFilePath']}") String jsonFilePath) {
        log.info("::: [{}] method Start :::", Thread.currentThread().getStackTrace()[1].getMethodName());
        return new JsonItemReaderBuilder<CommunityContents>()
                .name("jsonItemReader")
                .resource(new FileSystemResource(jsonFilePath))
                .jsonObjectReader(new JacksonJsonObjectReader<>(CommunityContents.class))
                .build();
    }

    /**
     * data 가공
     * @return ItemProcessor
     */
    @Bean
    public ItemProcessor<CommunityContents, Map<String, Object>> openSearchItemProcessor() {
        return item -> {
            log.info("::: [{}] method Start :::", Thread.currentThread().getStackTrace()[1].getMethodName());
            Map<String, Object> resultMap = new HashMap<>();

            resultMap.put(Constants.INDEX_URL, item.get_id());
            resultMap.put(Constants.INDEX_CATEGORY, item.getCategory());
            resultMap.put(Constants.INDEX_CONTENT, item.getContent());
            resultMap.put(Constants.INDEX_DESCRIPTION, item.getDescription());
            resultMap.put(Constants.INDEX_KEY_CODE, item.getKeyCode());
            resultMap.put(Constants.INDEX_NAME, item.getName());
            resultMap.put(Constants.INDEX_PUBLISH_DATE, item.getPublishDate());
            resultMap.put(Constants.INDEX_TITLE, item.getTitle());

            return resultMap;
        };
    }

    /**
     * opensearch 를 사용하여 색인
     * @param indexName 색인 명
     * @return  ItemWriter
     */
    @Bean
    @StepScope
    public ItemWriter<Map<String, Object>> openSearchItemWriter(@Value("#{jobParameters['indexName']}") String indexName) {
        return new ItemWriter<Map<String, Object>>() {

            @Override
            public void write(Chunk<? extends Map<String, Object>> chunk) throws Exception {
                log.info("::: [{}] method Start :::", Thread.currentThread().getStackTrace()[1].getMethodName());

                OpenSearchClientManager clientManager = new OpenSearchClientManager();
                client = clientManager.createClient(opensearchIp, opensearchPort, Constants.HTTP);

                BulkProcessor bulkProcessor = BulkProcessorInitializer.initializeBulkProcessor(client, 1000);

                for (Map<String, Object> item : chunk) {
                    IndexRequest indexRequest = new IndexRequest(indexName);
                    indexRequest.id(UUIDGenerator.generateUUID());
                    indexRequest.source(item, XContentType.JSON);
                    bulkProcessor.add(indexRequest);

//                    testCount++;
//                    if (testCount > 2013){
//                        throw new RuntimeException("강제 예외 발생");
//                    }
                }

                clientManager.closeClient(client);
            }
        };
    }

    /**
     * 색인한 json 파일 백업
     * @param jsonFilePath 색인 파일 위치
     * @return Tasklet
     */
    @Bean
    @StepScope
    public Tasklet fileBackupTasklet(@Value("#{jobParameters['jsonFilePath']}") String jsonFilePath) {
        return (contribution, chunkContext) -> {
            File backupDir = new File(indexDocsPathBackup);

            if (!backupDir.exists()) {
                backupDir.mkdirs();
            }

            if (StringUtils.isNotBlank(jsonFilePath)) {
                Path sourcePath = Paths.get(jsonFilePath);
                Path backupPath = Paths.get(indexDocsPathBackup + indexDocsPathSeparator + sourcePath.getFileName());
                try {
                    Files.move(sourcePath, backupPath, StandardCopyOption.ATOMIC_MOVE);
                    log.info("File backup complete... sourceDir: {}, backupDir: {}", sourcePath.toString(), backupPath.toString());
                } catch (IOException e) {
                    log.info("File backup failed... sourceDir: {}, backupDir: {}", sourcePath.toString(), backupDir.toString());
                }
            } else {
                log.info("File parameter does not exist... jsonFilePath: {}", jsonFilePath);
            }

            return RepeatStatus.FINISHED;
        };
    }
}
