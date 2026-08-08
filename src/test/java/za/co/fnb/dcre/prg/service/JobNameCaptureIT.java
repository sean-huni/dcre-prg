package za.co.fnb.dcre.prg.service;

import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.CockroachContainer;
import org.testcontainers.utility.DockerImageName;
import za.co.fnb.dcre.prg.data.model.PrgReportEntity;
import za.co.fnb.dcre.prg.data.repo.PrgReportRepo;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SCRUM-58 file-trace (Task 10): prg_report.job_name is the clock-scoped
 * trace-join anchor, populated in the SAME transaction as every report row.
 * (1) a SCHEDULED delta, (2) a HEARTBEAT quiet window and (3) an IMMEDIATE
 * parent report each persist a non-null job_name equal to the run's resolved
 * seam name (JOB_NAME env absent in CI, so {@code local-prg-<executionId>});
 * (4) kill-resume: two executions of the SAME (client, window) identity
 * (distinct execution ids -> distinct seam names) leave EXACTLY ONE report
 * row, keyed by the unique file_name, and the FIRST run's job_name stands
 * (the find-or-save restart no-op never overwrites it).
 * Own CockroachDB container per class; one configured client per kind.
 */
@SpringBootTest(properties = {"spring.batch.job.enabled=false", "dcre.exchange-root=build/test-exchange",
        "DCRE_EXCHANGE_ROOT=build/test-exchange"})
class JobNameCaptureIT {

    static final CockroachContainer CRDB =
            new CockroachContainer(DockerImageName.parse("cockroachdb/cockroach:v26.2.3"));

    static {
        CRDB.start();
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", CRDB::getJdbcUrl);
        registry.add("spring.datasource.username", CRDB::getUsername);
        registry.add("spring.datasource.password", CRDB::getPassword);
    }

    @Autowired
    Job prgJob;

    @Autowired
    JobOperator jobOperator;

    @Autowired
    JdbcTemplate jdbc;

    @Autowired
    PsrReportService service;

    @Autowired
    PrgReportRepo reports;

    // --- seed helpers (PrgJobTest / ImmediateReportIT shape) ---

    UUID parent(String client, String msgId) {
        UUID arrival = UUID.randomUUID();
        jdbc.update("INSERT INTO tx_header (arrival_id, msg_id_raw, msg_id, created_ts, tx_count,"
                        + " initg_pty, business_date, client_token, layout_version) VALUES (?,?,?,?,?,?,?,?,?)",
                arrival, msgId, msgId, "20260716080000", 1, client, "20260716", client, 2);
        return arrival;
    }

    void tx(UUID arrival, int seq, String e2e) {
        jdbc.update("INSERT INTO tx_entry (arrival_id, sequence, record_type, e2e_raw, e2e,"
                        + " creditor_account, currency, amount_raw, amount) VALUES (?,?,?,?,?,?,?,?,?)",
                arrival, seq, "DC", e2e, e2e, "62000000010", "ZAR", "1000", 10.00);
        jdbc.update("INSERT INTO validation_log (arrival_id, sequence, outcome) VALUES (?,?,'PASS')",
                arrival, seq);
    }

    UUID group(UUID arrival, String client, String msgId) {
        UUID id = UUID.randomUUID();
        jdbc.update("INSERT INTO prw_emission_group (id, arrival_id, client, source_msg_id,"
                        + " applied_max, total_tx, total_amount, expected_batch_count, split)"
                        + " VALUES (?,?,?,?,?,?,?,?,?)",
                id, arrival, client, msgId, 5000, 1L, 10.00, 1, false);
        return id;
    }

    UUID batch(UUID groupId, UUID arrival, String outboundMsgId) {
        UUID id = UUID.randomUUID();
        jdbc.update("INSERT INTO prw_emission (id, arrival_id, file_name, state, group_id,"
                        + " batch_ordinal, outbound_msg_id, visible_at) VALUES (?,?,?,?,?,?,?,now())",
                id, arrival, outboundMsgId + "_PAIN008.xml", "VISIBLE", groupId, 1, outboundMsgId);
        return id;
    }

    void member(UUID emissionId, int seq, String e2e) {
        jdbc.update("INSERT INTO prw_emission_member (emission_id, sequence, e2e, amount) VALUES (?,?,?,?)",
                emissionId, seq, e2e, 10.00);
    }

    /**
     * Correlated reply. PIX/PSX/PPX carry no emission_id column, so the ONLY
     * path from a reply back to its batch is
     * orgnl_msg_id = prw_emission.outbound_msg_id: the reply must be seeded
     * with THAT batch's outbound identity or it does not join at all.
     */
    void resp(String table, UUID emissionId, String e2e, String status) {
        resp(table, outboundOf(emissionId), e2e, status);
    }

    /** outbound_msg_id is globally unique (uq_prw_emission_outbound_msg), so the bind is exact. */
    String outboundOf(UUID emissionId) {
        return jdbc.queryForObject("SELECT outbound_msg_id FROM prw_emission WHERE id = ?",
                String.class, emissionId);
    }

    void resp(String table, String orgnlMsgId, String e2e, String status) {
        jdbc.update("INSERT INTO " + table + " (response_file, orgnl_msg_id, e2e, status)"
                + " VALUES (?,?,?,?)", "RESP_" + table + "_" + e2e + ".xml", orgnlMsgId, e2e, status);
    }

    /** Smallest reportable parent: one member with a terminal PBSR response. */
    void seedTerminalParent(String client, String msgId, String e2e) {
        UUID arrival = parent(client, msgId);
        tx(arrival, 1, e2e);
        UUID b = batch(group(arrival, client, msgId), arrival, msgId);
        member(b, 1, e2e);
        resp("pbsr_resp", b, e2e, "ACSC");
    }

    /**
     * No-emission family fallback (PrgJobTest shape): this arrival gets NO
     * prw_emission row, so the reply projects through
     * orgnl_msg_id = tx_header.msg_id instead of an outbound batch identity.
     */
    void seedScheduledDelta(String client, String msgId, String e2e) {
        UUID arrival = parent(client, msgId);
        tx(arrival, 1, e2e);
        resp("pbsr_resp", msgId, e2e, "ACSC");
    }

    JobParameters clockWindow(String client, String window) {
        return new JobParametersBuilder()
                .addString("client", client, true)
                .addString("window", window, true)
                .toJobParameters();
    }

    String jobName(String fileName) {
        return jdbc.queryForObject("SELECT job_name FROM prg_report WHERE file_name = ?",
                String.class, fileName);
    }

    String reportType(String fileName) {
        return jdbc.queryForObject("SELECT type FROM prg_report WHERE file_name = ?",
                String.class, fileName);
    }

    Path out(String client, String fileName) {
        return Path.of("build/test-exchange", client.toLowerCase(), "onhost-resp", "out", fileName);
    }

    void cleanExchange(String client, String... fileNames) throws Exception {
        for (String fileName : fileNames) {
            Files.deleteIfExists(out(client, fileName));
        }
    }

    // --- (1) SCHEDULED delta persists the seam job name ---

    @Test
    void scheduledDeltaReportPersistsNonNullJobName() throws Exception {
        String client = "FNBT01";
        cleanExchange(client, client + "_PSR_jn-sch.txt");
        seedScheduledDelta(client, "MSGJNSCH", "E2EJNSCH");

        JobExecution exec = jobOperator.start(prgJob, clockWindow(client, "jn-sch"));

        assertThat(exec.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(reportType(client + "_PSR_jn-sch.txt")).isEqualTo("SCHEDULED");
        assertThat(jobName(client + "_PSR_jn-sch.txt")).isEqualTo("local-prg-" + exec.getId());
    }

    // --- (2) HEARTBEAT quiet window persists the seam job name ---

    @Test
    void heartbeatReportPersistsNonNullJobName() throws Exception {
        String client = "FNBT02";
        cleanExchange(client, client + "_PSR_jn-hb.txt");

        JobExecution exec = jobOperator.start(prgJob, clockWindow(client, "jn-hb"));

        assertThat(exec.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(reportType(client + "_PSR_jn-hb.txt")).isEqualTo("HEARTBEAT");
        assertThat(jobName(client + "_PSR_jn-hb.txt")).isEqualTo("local-prg-" + exec.getId());
    }

    // --- (3) IMMEDIATE parent report persists the seam job name ---

    @Test
    void immediateReportPersistsNonNullJobName() throws Exception {
        String client = "FNBT03";
        cleanExchange(client, client + "_PSR_jn-imm.txt");
        seedTerminalParent(client, "MSGJNIMM", "E2EJNIMM");

        JobParameters params = new JobParametersBuilder()
                .addString("client", client, true)
                .addString("window", "jn-imm", true)
                .addString("report.type", "IMMEDIATE", false)
                .addString("parents", "MSGJNIMM", false)
                .toJobParameters();

        JobExecution exec = jobOperator.start(prgJob, params);

        assertThat(exec.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(reportType(client + "_PSR_jn-imm.txt")).isEqualTo("IMMEDIATE");
        assertThat(jobName(client + "_PSR_jn-imm.txt")).isEqualTo("local-prg-" + exec.getId());
    }

    // --- (4) kill-resume: one row per report identity, first execution's job_name stands ---

    @Test
    void killResumeLeavesExactlyOneRowPerReportIdentity() throws Exception {
        String client = "FNBT04";
        String file = client + "_PSR_jn-kr.txt";
        cleanExchange(client, file);

        // pre-kill execution stamps the seam name write-ahead of the file
        service.window(client, "jn-kr", false, "local-prg-100");
        // resume under a NEW execution id: find-or-save on the unique file_name reuses the row
        service.window(client, "jn-kr", false, "local-prg-200");

        assertThat(jdbc.queryForObject("SELECT count(*) FROM prg_report WHERE file_name = ?",
                Long.class, file)).isEqualTo(1L);
        assertThat(jobName(file)).isEqualTo("local-prg-100"); // first run wins; resume never overwrites
        assertThat(reports.findByJobName("local-prg-100")).extracting(PrgReportEntity::getFileName)
                .containsExactly(file);
        assertThat(reports.findByJobName("local-prg-200")).isEmpty(); // the resumed run minted no new row
    }
}
