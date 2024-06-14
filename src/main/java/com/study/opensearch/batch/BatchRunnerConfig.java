package com.study.opensearch.batch;

import com.study.opensearch.util.Constants;
import com.study.opensearch.util.OpenSearchClientManager;
import com.study.opensearch.util.OpenSearchIndexManager;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.client.RestHighLevelClient;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Slf4j
//@Configuration
@Component
public class BatchRunnerConfig {

    private final JobLauncher jobLauncher;

    private final Job manageLogJob;

    private final Job flowJob;

    private final Job mongoToJsonJob;

    private final Job mongoToJsonByRowJob;

    private final Job mongoToBulkJsonByRowJob;

    private final Job openSearchBatchJob;

    private final Job mongoToBulkJsonJob;

    OpenSearchIndexManager openSearchIndexManager = new OpenSearchIndexManager();

    RestHighLevelClient client = null;

    @Value("${schedule.mongoToJsonJob.use}")
    private boolean useSchedule;

    @Value("${index.docs.path.source}")
    private String indexDocsPath;

    @Value("${index.docs.name}")
    private String indexDocsName;

    @Value("${opensearch.ip}")
    private String opensearchIp;

    @Value("${opensearch.port}")
    private int opensearchPort;

    @Value("${index.docs.indexName}")
    private String indexDocsIndexName;

    public BatchRunnerConfig(JobLauncher jobLauncher,
                             @Qualifier("manageLogJob") Job manageLogJob,
                             @Qualifier("mongoToJsonJob") Job mongoToJsonJob,
                             @Qualifier("flowJob") Job flowJob,
                             @Qualifier("mongoToJsonByRowJob") Job mongoToJsonByRowJob,
                             @Qualifier("mongoToBulkJsonByRowJob") Job mongoToBulkJsonByRowJob,
                             @Qualifier("openSearchBatchJob") Job openSearchBatchJob,
                             @Qualifier("mongoToBulkJsonJob") Job mongoToBulkJsonJob) {
        this.jobLauncher = jobLauncher;
        this.manageLogJob = manageLogJob;
        this.flowJob = flowJob;
        this.mongoToJsonJob = mongoToJsonJob;
        this.mongoToJsonByRowJob = mongoToJsonByRowJob;
        this.mongoToBulkJsonByRowJob = mongoToBulkJsonByRowJob;
        this.openSearchBatchJob = openSearchBatchJob;
        this.mongoToBulkJsonJob = mongoToBulkJsonJob;
    }

    /**
     * manageLogJob 실행
     * @return
     */
//    @Bean
//    public CommandLineRunner run() {
//        log.info("::: [{}] method Start :::",  Thread.currentThread().getStackTrace()[1].getMethodName());
//
//        return args -> {
//            JobParameters jobParameters = new JobParametersBuilder()
//                    .addString("message", "Delete log data 1 day ago.!")
//                    .addString("days", "1")
//                    .toJobParameters();
//            jobLauncher.run(manageLogJob, jobParameters);
//        };
//    }

    /**
     * flowJob 실행
     * @return
     */
//    @Bean
//    public CommandLineRunner run() {
//        log.info("::: [{}] method Start :::", Thread.currentThread().getStackTrace()[1].getMethodName());
//
//        return args -> {
//            JobParameters jobParameters = new JobParametersBuilder()
//                    .addString("id", String.valueOf(Math.random()))
//                    .toJobParameters();
//            jobLauncher.run(flowJob, jobParameters);
//        };
//    }

    /**
     * mongoToJsonJob 실행
     * @return
     */
//    @Bean
//    public CommandLineRunner run() {
//        log.info("::: [{}] method Start :::",  Thread.currentThread().getStackTrace()[1].getMethodName());
//
//        LocalDateTime date = LocalDateTime.now();
//        String message = "Execute mongoToJsonJob at " + date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"));
//        String currentDate = date.format(DateTimeFormatter.ofPattern("yyyyMMddHHmm"));
//
//        return args -> {
//            JobParameters jobParameters = new JobParametersBuilder()
//                    .addString("keyCode", "BLIND")
//                    .addString("currentDate", currentDate)
//                    .addString("message", message)
//                    .toJobParameters();
//
//            jobLauncher.run(mongoToJsonJob, jobParameters);
//        };
//    }

    /**
     * mongoToJsonJob 실행 (스케줄러 적용)
     */
//    @Scheduled(fixedDelay = 300000)
//    @Scheduled(cron = "${schedule.mongoToJsonJob.cron}")
//    public void run() {
//        log.info("::: [{}] method Start :::", Thread.currentThread().getStackTrace()[1].getMethodName());
//
//        if(!useSchedule) {
//            return;
//        }
//
//        LocalDateTime date = LocalDateTime.now();
//        String message = "Execute mongoToJsonJob at " + date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"));
//        String currentDate = date.format(DateTimeFormatter.ofPattern("yyyyMMddHHmm"));
//
//        try{
//            JobParameters jobParameters = new JobParametersBuilder()
//                    .addString("keyCode", "BLIND")
//                    .addString("currentDate", currentDate)
//                    .addString("message", message)
//                    .toJobParameters();
//
//            jobLauncher.run(mongoToJsonJob, jobParameters);
//        }catch (Exception e){
//            log.error("mongoToJsonJob Error : {}", e.getMessage());
//        }
//    }

    /**
     * mongoToJsonByRowJob 실행
     */
//    @Bean
//    public CommandLineRunner run() {
//        log.info("::: [{}] method Start :::",  Thread.currentThread().getStackTrace()[1].getMethodName());
//
//        LocalDateTime date = LocalDateTime.now();
//        String message = "Execute mongoToJsonByRowJob at " + date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"));
//        String currentDate = date.format(DateTimeFormatter.ofPattern("yyyyMMddHHmm"));
//
//        return args -> {
//            JobParameters jobParameters = new JobParametersBuilder()
//                    .addString("currentDate", currentDate)
//                    .addString("message", message)
//                    .toJobParameters();
//
////            jobLauncher.run(mongoToJsonByRowJob, jobParameters);
//            jobLauncher.run(mongoToBulkJsonByRowJob, jobParameters);
//        };
//    }

    /**
     * mongoToBulkJsonJob 실행
     */
//    @Bean
//    public CommandLineRunner run() {
//        log.info("::: [{}] method Start :::",  Thread.currentThread().getStackTrace()[1].getMethodName());
//
//        LocalDateTime date = LocalDateTime.now();
//        String message = "Execute mongoToBulkJsonJob at " + date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"));
//        String currentDate = date.format(DateTimeFormatter.ofPattern("yyyyMMddHHmm"));
//
//        return args -> {
//            JobParameters jobParameters = new JobParametersBuilder()
////                    .addString("keyCode", "BLIND")
//                    .addString("currentDate", currentDate)
////                    .addString("message", message)
//                    .toJobParameters();
//
//            jobLauncher.run(mongoToBulkJsonJob, jobParameters);
//        };
//    }

    /**
     * open search 를 활용한 색인 작업
     * json to index
     */
//    @Bean
//    @Scheduled(fixedDelay = 300000)
//    public void run() throws IOException {
//        log.info("::: [{}] method Start :::", Thread.currentThread().getStackTrace()[1].getMethodName());
//
//        if(!useSchedule) {
//            return;
//        }
//
//        LocalDateTime date = LocalDateTime.now();
//        String message = "Execute mongoToJsonJob at " + date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"));
//        String currentDate = date.format(DateTimeFormatter.ofPattern("yyyyMMddHHmm"));
//
//        FilenameFilter filter = new FilenameFilter() {
//            @Override
//            public boolean accept(File file, String name) {
//                return name.contains(indexDocsName);// && file.getName().endsWith("json");
//            }
//        };
//
//        File[] jsonFiles = new File(indexDocsPath).listFiles(filter);
//
//        if(jsonFiles.length < 1){
//            log.info("There is no json file to index... fileName: {}", indexDocsPath+indexDocsName);
//            return;
//        }
//        // open search
////        String indexName = indexDocsIndexName + "_" + currentDate;
//        try{
//            OpenSearchClientManager clientManager = new OpenSearchClientManager();
//            client = clientManager.createClient(opensearchIp, opensearchPort, Constants.HTTP);
//
//            if (openSearchIndexManager.isExistIndex(indexDocsIndexName, client))
//                openSearchIndexManager.deleteIndex(indexDocsIndexName, client);
//
//            openSearchIndexManager.createIndex(indexDocsIndexName, client);
//
//            clientManager.closeClient(client);
//        }catch(Exception e){
//            log.error("Error Occurred... message: {}", e.getMessage());
//        }
//
//        for(File jsonFile : jsonFiles){
//            try{
//                JobParameters jobParameters = new JobParametersBuilder()
//                        .addString("currentDate", currentDate)
//                        .addString("jsonFilePath", jsonFile.toString())
//                        .addString("indexName", indexDocsIndexName)
//                        .toJobParameters();
//
//                jobLauncher.run(openSearchBatchJob, jobParameters);
//            }catch (Exception e){
//                log.error("mongoToJsonJob Error : {}", e.getMessage());
//            }
//        }
//
//        log.info("::: [{}] method End!!!!! :::", Thread.currentThread().getStackTrace()[1].getMethodName());
//    }
}


