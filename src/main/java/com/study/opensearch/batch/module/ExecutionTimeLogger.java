package com.study.opensearch.batch.module;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.stereotype.Component;

/**
 * job, step 실행 시간 계산
 */
@Slf4j
@Component
public class ExecutionTimeLogger implements JobExecutionListener, StepExecutionListener {

    private long startTime;

    @Override
    public void beforeJob(JobExecution jobExecution) {
        startTime = System.currentTimeMillis();
        log.info("Job execution started at: {}", startTime);
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        long endTime = System.currentTimeMillis();
        long executionTime = endTime - startTime;
        log.info("Job execution finished at: {}", endTime);
        log.info("Total execution time: {} milliseconds", executionTime);
    }

    @Override
    public void beforeStep(StepExecution stepExecution) {
        startTime = System.currentTimeMillis();
        log.info("Step execution started at: {}", startTime);
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        long endTime = System.currentTimeMillis();
        long executionTime = endTime - startTime;
        log.info("Step execution finished at: {}", endTime);
        log.info("Step execution time: {} milliseconds", executionTime);
        return null;
    }
}
