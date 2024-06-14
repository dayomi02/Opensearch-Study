package com.study.opensearch.batch;

import com.study.opensearch.domain.ApplicationLogEntity;
import com.study.opensearch.domain.ApplicationLogEntityRepository;
import com.study.opensearch.util.Constants;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.AdviceMode;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import java.time.LocalDateTime;

@Slf4j
@Configuration
@RequiredArgsConstructor
@EnableTransactionManagement(mode = AdviceMode.ASPECTJ)
public class ManageLogJobConfig {

    private final JobRepository jobRepository;

    private final JpaTransactionManager transactionManager;

    private final ApplicationLogEntityRepository applicationLogEntityRepository;

    /**
     * Manage logs Job
     * @return Manage logs job
     */
    @Bean
    public Job manageLogJob() {
        log.info("::: [{}] method Start :::",  Thread.currentThread().getStackTrace()[1].getMethodName());
        return new JobBuilder("manageLogJob", jobRepository)
                .start(registerLogStep())       // Job 이 처음 시작될 때 실행되는 step.
                .next(deleteLogStep())          // 이전 step 이 끝나면 실핼되는 step.
//                .start(transactionTestStep())
                .build();                       // Job 을 빌드하고 최종 Job 객체를 생성.
    }

    /**
     * Tasklet 을 활용한 로그 등록 Step
     *
     * @return Log register Step
     */
    @Bean
    public Step registerLogStep() {
        log.info("::: [{}] method Start :::",  Thread.currentThread().getStackTrace()[1].getMethodName());

        return new StepBuilder("registerLogStep", jobRepository)
                .tasklet(registerLogTasklet(null), transactionManager)      // Tasklet 설정.
                .build();
    }

    /**
     * 로그 등록 Tasklet
     *
     * @param message Log message
     * @return Log register Tasklet
     */
    @Bean
    @StepScope
    public Tasklet registerLogTasklet(@Value("#{jobParameters['message']}") String message) {
        log.info("::: [{}] method Start :::",  Thread.currentThread().getStackTrace()[1].getMethodName());

        return (contribution, chunkContext) -> {
            ApplicationLogEntity logEntity = ApplicationLogEntity.builder()
                    .timestamp(LocalDateTime.now())
                    .level(Constants.LOG_LEVEL_INFO)
                    .message(message)
                    .userId(Constants.LOG_USER_ADMIN).build();

            applicationLogEntityRepository.save(logEntity);

            return RepeatStatus.FINISHED;       // 작업 완료
        };
    }

    /**
     * Tasklet 을 활용한 로그 삭제 Step
     * @return Log delete Step
     */
    @Bean
    public Step deleteLogStep() {
        log.info("::: [{}] method Start :::",  Thread.currentThread().getStackTrace()[1].getMethodName());

        return new StepBuilder("deleteLogStep", jobRepository)
                .tasklet(deleteLogTasklet(null), transactionManager)
                .build();
    }

    /**
     * 로그 삭제 Tasklet
     * @param days 삭제일
     * @return Log delete Tasklet
     */
    @Bean
    @StepScope
    public Tasklet deleteLogTasklet(@Value("#{jobParameters['days']}") String days) {
        log.info("::: [{}] method Start :::",  Thread.currentThread().getStackTrace()[1].getMethodName());

        return (contribution, chunkContext) -> {
            LocalDateTime dayBefore = LocalDateTime.now().minusDays(Long.parseLong(days));

            applicationLogEntityRepository.deleteLogBefore(dayBefore);
            log.info("::: Delete logs prior to {} :::", dayBefore);

            try {
                log.info("3초 대기...");

                Thread.sleep(3000);

                throw new RuntimeException("강제 예외 발생"); // 예외 발생
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            return RepeatStatus.FINISHED;
        };
    }

    /**
     * Tasklet 을 활용한 트랜잭션 테스트 Step
     * @return transaction test step
     */
    @Bean
    public Step transactionTestStep() {
        log.info("::: [{}] method Start :::",  Thread.currentThread().getStackTrace()[1].getMethodName());

        return new StepBuilder("transactionTestStep", jobRepository)
                .tasklet(transactionTestTasklet(null, null), transactionManager)
                .build();
    }

    /**
     * 트랜잭션 테스트 Tasklet
     * @param message log message
     * @param days 삭제 일
     * @return transaction test tasklet
     */
    @Bean
    @StepScope
    public Tasklet transactionTestTasklet(@Value("#{jobParameters['message']}") String message, @Value("#{jobParameters['days']}") String days) {
        log.info("::: [{}] method Start :::",  Thread.currentThread().getStackTrace()[1].getMethodName());

        return (contribution, chunkContext) -> {

            ApplicationLogEntity logEntity = ApplicationLogEntity.builder()
                    .timestamp(LocalDateTime.now())
                    .level(Constants.LOG_LEVEL_INFO)
                    .message(message)
                    .userId(Constants.LOG_USER_ADMIN).build();

            applicationLogEntityRepository.save(logEntity);

            try {
                log.info("3초 대기...");

                Thread.sleep(3000);

                throw new RuntimeException("강제 예외 발생"); // 예외 발생
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            LocalDateTime dayBefore = LocalDateTime.now().minusDays(Long.parseLong(days));

            applicationLogEntityRepository.deleteLogBefore(dayBefore);
            log.info("::: Delete logs prior to {} :::", dayBefore);



            return RepeatStatus.FINISHED;
        };
    }
}
