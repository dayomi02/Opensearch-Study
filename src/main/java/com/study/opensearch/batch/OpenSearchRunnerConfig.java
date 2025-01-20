package com.study.opensearch.batch;

import com.study.opensearch.util.Constants;
import com.study.opensearch.util.OpenSearchIndexManager;
import lombok.extern.slf4j.Slf4j;
import org.h2.schema.Constant;
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
public class OpenSearchRunnerConfig {

    private final JobLauncher jobLauncher;

    private final Job mongoToBulkJsonByRowJob;

    private final Job openSearchBatchJob;

    private final Job mongoToBulkJsonJob;

    @Value("${schedule.mongoToBulkJsonJob.use}")
    private boolean bulkJsonJobUseSchedule;

    @Value("${schedule.openSearchBatchJob.use}")
    private boolean batchJobUseSchedule;

    @Value("${schedule.byRowJobUseSchedule.use}")
    private boolean byRowJobUseSchedule;

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

    public OpenSearchRunnerConfig(JobLauncher jobLauncher,
                                  @Qualifier("mongoToBulkJsonByRowJob") Job mongoToBulkJsonByRowJob,
                                  @Qualifier("openSearchBatchJob") Job openSearchBatchJob,
                                  @Qualifier("mongoToBulkJsonJob") Job mongoToBulkJsonJob) {
        this.jobLauncher = jobLauncher;
        this.mongoToBulkJsonByRowJob = mongoToBulkJsonByRowJob;
        this.openSearchBatchJob = openSearchBatchJob;
        this.mongoToBulkJsonJob = mongoToBulkJsonJob;
    }

    /**
     * mongoToBulkJsonByRowJob 실행
     */
    @Bean
    @Scheduled(cron = "${schedule.byRowJobUseSchedule.cron}")
    public void byRowrun() throws IOException {
        log.info(Constants.LOG_METHOD_START,  Thread.currentThread().getStackTrace()[1].getMethodName());

        if (!byRowJobUseSchedule) {
            return;
        }

        LocalDateTime date = LocalDateTime.now();
        String message = "Execute mongoToJsonByRowJob at " + date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"));
        String formatDate = date.format(DateTimeFormatter.ofPattern(Constants.DATE_FORMAT));

        try{
            JobParameters jobParameters = new JobParametersBuilder()
                    .addString(Constants.JOB_PARAMETER_CURRENT_DATE, formatDate)
                    .addString("message", message)
                    .toJobParameters();

//            jobLauncher.run(mongoToJsonByRowJob, jobParameters);
            jobLauncher.run(mongoToBulkJsonByRowJob, jobParameters);
        } catch (Exception e) {
            log.error("Error Occurred... message: {}", e.getMessage());
        }
    }

    /**
     * mongoToBulkJsonJob 실행
     */
    @Bean
    @Scheduled(cron = "${schedule.mongoToBulkJsonJob.cron}")
    public void run() throws IOException {
        log.info(Constants.LOG_METHOD_START, Thread.currentThread().getStackTrace()[1].getMethodName());

        if (!bulkJsonJobUseSchedule) {
            return;
        }

        LocalDateTime date = LocalDateTime.now();
        String message = "Execute mongoToBulkJsonJob at " + date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"));
        String currentDate = date.format(DateTimeFormatter.ofPattern(Constants.DATE_FORMAT));

        try {
            JobParameters jobParameters = new JobParametersBuilder()
                    .addString(Constants.JOB_PARAMETER_CURRENT_DATE, currentDate)
                    .addString("message", message)
                    .toJobParameters();

            jobLauncher.run(mongoToBulkJsonJob, jobParameters);

        } catch (Exception e) {
            log.error("Error Occurred... message: {}", e.getMessage());
        }
    }

    /**
     * openSearchBatchJob 실행
     */
    @Bean
    @Scheduled(cron = "${schedule.openSearchBatchJob.cron}")
    public void runIndex() throws IOException {
        log.info(Constants.LOG_METHOD_START, Thread.currentThread().getStackTrace()[1].getMethodName());

        if(!batchJobUseSchedule) {
            return;
        }

        LocalDateTime date = LocalDateTime.now();
        String currentDate = date.format(DateTimeFormatter.ofPattern(Constants.DATE_FORMAT));

        FilenameFilter filter = (File file, String name) -> name.contains(indexDocsName);

        File[] jsonFiles = new File(indexDocsPath).listFiles(filter);

        if(jsonFiles.length < 1) {
            log.info("There is no json file to index... fileName: {}", indexDocsPath + indexDocsName);
            return;
        }

        for(File jsonFile : jsonFiles){
            try{
                JobParameters jobParameters = new JobParametersBuilder()
                        .addString(Constants.JOB_PARAMETER_CURRENT_DATE, currentDate)
                        .addString(Constants.JOB_PARAMETER_JSON_FILE_PATH, jsonFile.toString())
                        .addString(Constants.JOB_PARAMETER_INDEX_NAME, indexDocsIndexName)
                        .toJobParameters();

                jobLauncher.run(openSearchBatchJob, jobParameters);
            }catch (Exception e){
                log.error("openSearchBatchJob Error : {}", e.getMessage());
            }
        }

        log.info("::: [{}] method End!!!!! :::", Thread.currentThread().getStackTrace()[1].getMethodName());
    }
}


