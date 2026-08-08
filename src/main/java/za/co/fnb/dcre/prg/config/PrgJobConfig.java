package za.co.fnb.dcre.prg.config;

import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.boot.ApplicationRunner;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.transaction.PlatformTransactionManager;
import za.co.fnb.dcre.platform.batch.HeartbeatWriter;
import za.co.fnb.dcre.platform.batch.OutcomeSeamListener;
import za.co.fnb.dcre.platform.batch.StaleExecutionSweeper;
import za.co.fnb.dcre.prg.service.PsrTasklet;

import javax.sql.DataSource;

@Configuration
public class PrgJobConfig {

    @Bean
    public Job prgJob(JobRepository repo, PlatformTransactionManager tx, PsrTasklet tasklet,
                      HeartbeatWriter heartbeatWriter,
                      @Value("${dcre.exchange-root}") String exchangeRoot) {
        Step psrStep = new StepBuilder("psrStep", repo).tasklet(tasklet, tx).build();
        // SCRUM-58: the shared seam listener replaces the per-module SeamListener
        // record. Verdict preserved byte-exact (BUSINESS_ACCEPTED); the shared
        // listener gates on COMPLETED and self-describes the local seam name as
        // local-prg-<executionId> when JOB_NAME is absent.
        return new JobBuilder("prgJob", repo)
                .listener(new OutcomeSeamListener("prg", exchangeRoot, execution -> "BUSINESS_ACCEPTED"))
                .listener(heartbeatWriter)
                .start(psrStep)
                .build();
    }

    @Bean
    @Order(-10)
    public ApplicationRunner staleExecutionSweep(DataSource dataSource) {
        return args -> StaleExecutionSweeper.abandonStale(dataSource, "PRG_BATCH_", 60);
    }
}
