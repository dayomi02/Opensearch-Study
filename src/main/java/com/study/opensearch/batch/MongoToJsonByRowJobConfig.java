package com.study.opensearch.batch;

import com.study.opensearch.batch.module.ExecutionTimeLogger;
import com.study.opensearch.batch.module.MongoToBulkJsonByRowTasklet;
import com.study.opensearch.batch.module.MongoToJsonByRowTasklet;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * MongoDB 데이터 1건 씩 Json 파일로 저장
 */
@Slf4j
@Configuration
public class MongoToJsonByRowJobConfig {

    private final JobRepository jobRepository;

    private final PlatformTransactionManager transactionManager;

    private final ExecutionTimeLogger executionTimeLogger;

    private final MongoToJsonByRowTasklet mongoToJsonByRowTasklet;

    private final MongoToBulkJsonByRowTasklet mongoToBulkJsonByRowTasklet;

    private final Step registerLogStep;


    public MongoToJsonByRowJobConfig(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                                     @Qualifier("executionTimeLogger") ExecutionTimeLogger executionTimeLogger,
                                     @Qualifier("mongoToJsonByRowTasklet") MongoToJsonByRowTasklet mongoToJsonByRowTasklet,
                                     @Qualifier("mongoToBulkJsonByRowTasklet") MongoToBulkJsonByRowTasklet mongoToBulkJsonByRowTasklet,
                                     @Qualifier("registerLogStep") Step registerLogStep){
        this.jobRepository = jobRepository;
        this.transactionManager = transactionManager;
        this.executionTimeLogger = executionTimeLogger;
        this.mongoToJsonByRowTasklet = mongoToJsonByRowTasklet;
        this.mongoToBulkJsonByRowTasklet = mongoToBulkJsonByRowTasklet;
        this.registerLogStep = registerLogStep;
    }

    /**
     * simple json 생성 job
     * @return job
     */
    @Bean
    public Job mongoToJsonByRowJob(){
        return new JobBuilder("mongoToJsonByRowJob", jobRepository)
                .start(mongoToJsonByRowStep())
                .next(registerLogStep)
                .listener(executionTimeLogger)
                .build();
    }

    /**
     * simple json 생성 step
     * @return step
     */
    @Bean
    public Step mongoToJsonByRowStep(){
        return new StepBuilder("mongoToJsonByRowStep", jobRepository)
                .tasklet(mongoToJsonByRowTasklet, transactionManager)
                .build();
    }

    @Bean
    public Job mongoToBulkJsonByRowJob(){
        return new JobBuilder("mongoToBulkJsonByRowJob", jobRepository)
                .start(mongoToBulkJsonByRowStep())
                .next(registerLogStep)
                .listener(executionTimeLogger)
                .build();
    }

    @Bean
    public Step mongoToBulkJsonByRowStep(){
        return new StepBuilder("mongoToBulkJsonByRowStep", jobRepository)
                .tasklet(mongoToBulkJsonByRowTasklet, transactionManager)
                .build();
    }

}
