package com.study.opensearch.batch;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.orm.jpa.JpaTransactionManager;

/**
 * Flow Job 테스트
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
public class FlowJobConfig {

    private final JobRepository jobRepository;

    private final JpaTransactionManager transactionManager;

    @Bean
    public Job flowJob() {
        return new JobBuilder("flowJob", jobRepository)
                // step1이 성공한다면 step3 실행
                // step1이 실패한다면 step2 실행
                .start(step1())
                .on("COMPLETED").to(step3())
                .from(step1())
                .on("FAILED").to(step2())
                .end()
                .build();
    }

    @Bean
    public Step step1() {
        return new StepBuilder("step1", jobRepository)
                .tasklet(tasklet1(), transactionManager)
                .build();
    }

    @Bean
    public Tasklet tasklet1() {
        return (contribution, chunkContext) -> {
            log.info("::: Call tasklet1 :::");

//            contribution.getStepExecution().setExitStatus(ExitStatus.FAILED);     // 강제 실패 처리

            return RepeatStatus.FINISHED;
        };
    }

    @Bean
    public Step step2() {
        return new StepBuilder("step2", jobRepository)
                .tasklet(tasklet2(), transactionManager)
                .build();
    }

    @Bean
    public Tasklet tasklet2() {
        return (contribution, chunkContext) -> {
            log.info("::: Call tasklet2 :::");
            return RepeatStatus.FINISHED;
        };
    }

    @Bean
    public Step step3() {
        return new StepBuilder("step3", jobRepository)
                .tasklet(tasklet3(), transactionManager)
                .build();
    }

    @Bean
    public Tasklet tasklet3() {
        return (contribution, chunkContext) -> {
            log.info("::: Call tasklet3 :::");
            return RepeatStatus.FINISHED;
        };
    }

}
