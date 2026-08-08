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
import za.co.fnb.dcre.prg.data.model.LedgerRow;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * SCRUM-55 Task 10 (spec tests 6 and 7): parent-scoped IMMEDIATE and MANUAL
 * report modes with the delivery-ledger auto guard.
 * (1) an immediate parent report carries exactly the unledgered statuses,
 *     ledgers them, advances the watermark, and a re-run is a file-less no-op;
 * (2) a status transition is reportable, the same status twice is not;
 * (3) manual replay reproduces the exact original file and ledgers every
 *     line again with a manual_ref (guard bypassed, still audited);
 * (4) the tasklet dispatches report.type=IMMEDIATE launches per parent with
 *     per-parent window keys (prg_report.file_name is unique);
 * (5) negative: an unknown report.type fails the job; an unknown report.id
 *     rejects the replay;
 * (6) MANUAL regenerate-from-current-status bypasses the auto guard, ledgers
 *     with the manual_ref and stamps `type` MANUAL (keyset-paginated:
 *     psr-slice-size is 2 here so 3 rows take two slices);
 * (7) the tasklet dispatches report.type=MANUAL + parents + manual.ref;
 * (8)+(9) kill-resume (Task 16 chaos contract): a restart after ledger slices
 *     committed but before the file's ATOMIC_MOVE re-renders the pre-crash
 *     ledgered lines instead of silently dropping them.
 * Clients come from the FNBT isolation pool + FNBCC01 (one per test: ledger
 * tuples and report file names are client-global).
 */
@SpringBootTest(properties = {"spring.batch.job.enabled=false", "dcre.exchange-root=build/test-exchange",
        "DCRE_EXCHANGE_ROOT=build/test-exchange", "dcre.prg.psr-slice-size=2"})
class ImmediateReportIT {

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
    JdbcTemplate jdbc;

    @Autowired
    ImmediateReportService immediate;

    @Autowired
    Job prgJob;

    @Autowired
    JobOperator jobOperator;

    // --- seed helpers (ReportingSchemaIT shape; non-owned tables exist via the 001 bootstrap guards) ---

    UUID parent(String client, String msgId) {
        UUID arrival = UUID.randomUUID();
        jdbc.update("INSERT INTO tx_header (arrival_id, msg_id_raw, msg_id, created_ts, tx_count,"
                        + " initg_pty, business_date, client_token, layout_version) VALUES (?,?,?,?,?,?,?,?,?)",
                arrival, msgId, msgId, "20260716080000", 1, client, "20260716", client, 2);
        return arrival;
    }

    /** validated=false leaves the row mid-DAG (no verdict -> status NULL -> never reportable). */
    void tx(UUID arrival, int seq, String e2e, boolean validated) {
        jdbc.update("INSERT INTO tx_entry (arrival_id, sequence, record_type, e2e_raw, e2e,"
                        + " creditor_account, currency, amount_raw, amount) VALUES (?,?,?,?,?,?,?,?,?)",
                arrival, seq, "DC", e2e, e2e, "62000000010", "ZAR", "1000", 10.00);
        if (validated) {
            jdbc.update("INSERT INTO validation_log (arrival_id, sequence, outcome) VALUES (?,?,'PASS')",
                    arrival, seq);
        }
    }

    /** No run_date on any prw_emission* table: payments never warehouses, so it is not part of any identity. */
    UUID group(UUID arrival, String client, String msgId, int batches) {
        UUID id = UUID.randomUUID();
        jdbc.update("INSERT INTO prw_emission_group (id, arrival_id, client, source_msg_id,"
                        + " applied_max, total_tx, total_amount, expected_batch_count, split)"
                        + " VALUES (?,?,?,?,?,?,?,?,?)",
                id, arrival, client, msgId, 5000, 2L, 20.00, batches, batches > 1);
        return id;
    }

    UUID batch(UUID groupId, UUID arrival, int ordinal, String outboundMsgId) {
        UUID id = UUID.randomUUID();
        jdbc.update("INSERT INTO prw_emission (id, arrival_id, file_name, state, group_id,"
                        + " batch_ordinal, outbound_msg_id, visible_at) VALUES (?,?,?,?,?,?,?,now())",
                id, arrival, outboundMsgId + "_PAIN008.xml", "VISIBLE", groupId, ordinal, outboundMsgId);
        return id;
    }

    void member(UUID emissionId, int seq, String e2e) {
        jdbc.update("INSERT INTO prw_emission_member (emission_id, sequence, e2e, amount) VALUES (?,?,?,?)",
                emissionId, seq, e2e, 10.00);
    }

    /**
     * Correlated reply. isr_resp/sbsr_resp/pbsr_resp carry NO emission_id column
     * (PIX/PSX/PPX dropped CIX's emission FK), so a reply binds to its batch through
     * orgnl_msg_id = prw_emission.outbound_msg_id and nothing else. Passing the batch
     * id here therefore means "this reply belongs to that batch", exactly as before.
     */
    void resp(String table, UUID emissionId, String e2e, String status) {
        resp(table, outboundOf(emissionId), e2e, status);
    }

    /** The one correlation path: outbound_msg_id is globally unique (uq_prw_emission_outbound_msg). */
    String outboundOf(UUID emissionId) {
        return jdbc.queryForObject("SELECT outbound_msg_id FROM prw_emission WHERE id = ?",
                String.class, emissionId);
    }

    void resp(String table, String orgnlMsgId, String e2e, String status) {
        jdbc.update("INSERT INTO " + table + " (response_file, orgnl_msg_id, e2e, status)"
                + " VALUES (?,?,?,?)", "RESP_" + table + "_" + e2e + ".xml", orgnlMsgId, e2e, status);
    }

    /** One member with a terminal PBSR response: the smallest reportable parent. */
    void seedTerminalParent(String client, String msgId, String e2e) {
        UUID arrival = parent(client, msgId);
        tx(arrival, 1, e2e, true);
        UUID b = batch(group(arrival, client, msgId, 1), arrival, 1, msgId);
        member(b, 1, e2e);
        resp("pbsr_resp", b, e2e, "ACSC");
    }

    List<LedgerRow> autoLedgerRows(String client) {
        return jdbc.query("SELECT e2e, status FROM prg_delivery_ledger WHERE client = ?"
                        + " AND manual_ref IS NULL ORDER BY e2e",
                (rs, i) -> new LedgerRow(rs.getString("e2e"), rs.getString("status")), client);
    }

    long manualLedgerCount(String client) {
        Long count = jdbc.queryForObject("SELECT count(*) FROM prg_delivery_ledger WHERE client = ?"
                + " AND manual_ref IS NOT NULL", Long.class, client);
        return count == null ? 0 : count;
    }

    UUID reportIdOf(Path file) {
        return jdbc.queryForObject("SELECT id FROM prg_report WHERE file_name = ?",
                UUID.class, file.getFileName().toString());
    }

    Path out(String client, String fileName) {
        return Path.of("build/test-exchange", client.toLowerCase(), "onhost-resp", "out", fileName);
    }

    void cleanExchange(String client, String... fileNames) throws Exception {
        for (String fileName : fileNames) {
            Files.deleteIfExists(out(client, fileName));
        }
    }

    // --- (1) spec test 6/7: immediate report = unledgered delta only, watermark advanced ---

    @Test
    void immediateParentReportSendsOnlyUnledgeredStatusesAndAdvancesWatermark() throws Exception {
        String client = "FNBCC01";
        cleanExchange(client, client + "_PSR_imm-1.txt", client + "_PSR_imm-2.txt");
        UUID arrival = parent(client, "MSGIMM1");
        UUID groupId = group(arrival, client, "MSGIMM1", 2);
        UUID b1 = batch(groupId, arrival, 1, "MSGIMM1_1");
        UUID b2 = batch(groupId, arrival, 2, "MSGIMM1_2");
        // batch 1: responses arrived, terminal statuses
        tx(arrival, 1, "E2EIMA1", true);
        member(b1, 1, "E2EIMA1");
        resp("pbsr_resp", b1, "E2EIMA1", "ACSC");
        tx(arrival, 2, "E2EIMA2", true);
        member(b1, 2, "E2EIMA2");
        resp("pbsr_resp", b1, "E2EIMA2", "RJCT");
        // batch 2: responses not yet arrived; rows stay mid-DAG (status NULL, PrgJobTest tx5 shape)
        tx(arrival, 3, "E2EIMB1", false);
        member(b2, 3, "E2EIMB1");
        tx(arrival, 4, "E2EIMB2", false);
        member(b2, 4, "E2EIMB2");

        var path = immediate.reportParent(client, "MSGIMM1", "imm-1");

        assertThat(path).isPresent();
        assertThat(Files.readAllLines(path.get())).containsExactly(
                "PSR|FNBCC01|imm-1", "TX|E2EIMA1|ACSC", "TX|E2EIMA2|RJCT", "END|2");
        assertThat(autoLedgerRows(client)).extracting(LedgerRow::e2e)
                .containsExactlyInAnyOrder("E2EIMA1", "E2EIMA2");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM prg_watermark WHERE client = ?",
                Long.class, client)).isEqualTo(2L); // immediate reports DO advance the watermark

        // second run: nothing new -> no file, no duplicate ledger rows
        assertThat(immediate.reportParent(client, "MSGIMM1", "imm-2")).isEmpty();
        assertThat(Files.exists(out(client, client + "_PSR_imm-2.txt"))).isFalse();
        assertThat(autoLedgerRows(client)).hasSize(2);
    }

    // --- (2) transition reportable, same status guarded ---

    @Test
    void statusTransitionIsReportableButSameStatusIsNot() throws Exception {
        String client = "FNBT00";
        cleanExchange(client, client + "_PSR_immt-1.txt", client + "_PSR_immt-2.txt",
                client + "_PSR_immt-3.txt");
        UUID arrival = parent(client, "MSGIMT");
        tx(arrival, 1, "E2EIMT1", true);
        UUID b = batch(group(arrival, client, "MSGIMT", 1), arrival, 1, "MSGIMT");
        member(b, 1, "E2EIMT1");
        resp("isr_resp", b, "E2EIMT1", "ACSP"); // interim reported once

        var first = immediate.reportParent(client, "MSGIMT", "immt-1");
        assertThat(first).isPresent();
        assertThat(Files.readAllLines(first.get())).contains("TX|E2EIMT1|ACSP");

        jdbc.update("UPDATE isr_resp SET status = 'ACSC' WHERE e2e = 'E2EIMT1'"); // transition
        var second = immediate.reportParent(client, "MSGIMT", "immt-2");
        assertThat(second).isPresent(); // ACSC line sent
        assertThat(Files.readAllLines(second.get())).contains("TX|E2EIMT1|ACSC");

        assertThat(immediate.reportParent(client, "MSGIMT", "immt-3")).isEmpty(); // ACSC again: guard holds
        assertThat(autoLedgerRows(client)).containsExactly(
                new LedgerRow("E2EIMT1", "ACSC"), new LedgerRow("E2EIMT1", "ACSP"));
    }

    // --- (3) manual replay: exact file, manual_ref audit, watermark untouched ---

    @Test
    void manualReplayReproducesExactReportAndLedgersWithManualRef() throws Exception {
        String client = "FNBT01";
        cleanExchange(client, client + "_PSR_immr-1.txt");
        UUID arrival = parent(client, "MSGIMR");
        tx(arrival, 1, "E2EIMR1", true);
        tx(arrival, 2, "E2EIMR2", true);
        UUID b = batch(group(arrival, client, "MSGIMR", 1), arrival, 1, "MSGIMR");
        member(b, 1, "E2EIMR1");
        member(b, 2, "E2EIMR2");
        resp("pbsr_resp", b, "E2EIMR1", "ACSC");
        resp("isr_resp", b, "E2EIMR2", "ACSP");

        var first = immediate.reportParent(client, "MSGIMR", "immr-1");
        assertThat(first).isPresent();
        UUID reportId = reportIdOf(first.get());

        Path replayed = immediate.replay(reportId);

        assertThat(replayed.getFileName().toString()).startsWith(client + "_PSR_immr-1.txt.replay-");
        assertThat(Files.readAllLines(replayed)).isEqualTo(Files.readAllLines(first.get()));
        assertThat(manualLedgerCount(client)).isEqualTo(2); // bypasses the guard, still audited
        assertThat(jdbc.queryForObject("SELECT count(*) FROM prg_report WHERE client = ?"
                + " AND type = 'MANUAL'", Long.class, client)).isEqualTo(1L);
        // replay renders history; it must never touch (or regress) the watermark
        assertThat(jdbc.queryForObject("SELECT last_status FROM prg_watermark WHERE client = ?"
                + " AND e2e = 'E2EIMR2'", String.class, client)).isEqualTo("ACSP");
    }

    // --- (4) tasklet dispatch: report.type=IMMEDIATE, parents comma-joined, per-parent window keys ---

    @Test
    void immediateJobLaunchReportsEachParentUnderItsOwnWindowKey() throws Exception {
        String client = "FNBT02";
        cleanExchange(client, client + "_PSR_immjob1-1.txt", client + "_PSR_immjob1-2.txt");
        seedTerminalParent(client, "MSGJOB1", "E2EJOB1");
        seedTerminalParent(client, "MSGJOB2", "E2EJOB2");
        JobParameters params = new JobParametersBuilder()
                .addString("client", client, true)
                .addString("window", "immjob1", true)
                .addString("report.type", "IMMEDIATE", false)
                .addString("parents", "MSGJOB1,MSGJOB2", false)
                .toJobParameters();

        JobExecution execution = jobOperator.start(prgJob, params);

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(Files.readAllLines(out(client, client + "_PSR_immjob1-1.txt"))).containsExactly(
                "PSR|" + client + "|immjob1-1", "TX|E2EJOB1|ACSC", "END|1");
        assertThat(Files.readAllLines(out(client, client + "_PSR_immjob1-2.txt"))).containsExactly(
                "PSR|" + client + "|immjob1-2", "TX|E2EJOB2|ACSC", "END|1");
        assertThat(autoLedgerRows(client)).containsExactly(
                new LedgerRow("E2EJOB1", "ACSC"), new LedgerRow("E2EJOB2", "ACSC"));
    }

    // --- (6) MANUAL regenerate: guard bypassed, keyset-paginated, manual_ref audited, MANUAL stamped ---

    @Test
    void manualRegenerateResendsAllCurrentStatusesBypassingTheGuard() throws Exception {
        String client = "FNBT12";
        cleanExchange(client, client + "_PSR_man-1.txt", client + "_PSR_man-2.txt");
        UUID arrival = parent(client, "MSGMAN");
        UUID b = batch(group(arrival, client, "MSGMAN", 1), arrival, 1, "MSGMAN");
        tx(arrival, 1, "E2EMAN1", true);
        member(b, 1, "E2EMAN1");
        resp("pbsr_resp", b, "E2EMAN1", "ACSC");
        tx(arrival, 2, "E2EMAN2", true);
        member(b, 2, "E2EMAN2");
        resp("pbsr_resp", b, "E2EMAN2", "RJCT");
        tx(arrival, 3, "E2EMAN3", true);
        member(b, 3, "E2EMAN3");
        resp("pbsr_resp", b, "E2EMAN3", "ACSC");
        assertThat(immediate.reportParent(client, "MSGMAN", "man-1")).isPresent(); // auto-ledgers all three

        var manual = immediate.reportParent(client, "MSGMAN", "man-2", "OPS-7");

        assertThat(manual).isPresent();
        // guard bypassed: every tuple is already auto-ledgered yet all three re-emit;
        // 3 rows at slice size 2 = two findCurrentForParent keyset slices
        assertThat(Files.readAllLines(manual.get())).containsExactly(
                "PSR|" + client + "|man-2", "TX|E2EMAN1|ACSC", "TX|E2EMAN2|RJCT",
                "TX|E2EMAN3|ACSC", "END|3");
        assertThat(jdbc.queryForObject("SELECT type FROM prg_report WHERE file_name = ?",
                String.class, client + "_PSR_man-2.txt")).isEqualTo("MANUAL");
        assertThat(jdbc.query("SELECT e2e, manual_ref FROM prg_delivery_ledger WHERE client = ?"
                        + " AND manual_ref IS NOT NULL ORDER BY e2e",
                (rs, i) -> rs.getString("e2e") + ":" + rs.getString("manual_ref"), client))
                .containsExactly("E2EMAN1:OPS-7", "E2EMAN2:OPS-7", "E2EMAN3:OPS-7");
        assertThat(autoLedgerRows(client)).hasSize(3); // no duplicate auto tuples minted
    }

    // --- (7) tasklet dispatch: report.type=MANUAL + parents + manual.ref ---

    @Test
    void manualJobLaunchRegeneratesLedgeredParentWithManualRef() throws Exception {
        String client = "FNBT13";
        cleanExchange(client, client + "_PSR_manjob0.txt", client + "_PSR_manjob1.txt");
        seedTerminalParent(client, "MSGMJ1", "E2EMJ1");
        assertThat(immediate.reportParent(client, "MSGMJ1", "manjob0")).isPresent(); // tuple auto-ledgered
        JobParameters params = new JobParametersBuilder()
                .addString("client", client, true)
                .addString("window", "manjob1", true)
                .addString("report.type", "MANUAL", false)
                .addString("parents", "MSGMJ1", false)
                .addString("manual.ref", "OPS-9", false)
                .toJobParameters();

        JobExecution execution = jobOperator.start(prgJob, params);

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(Files.readAllLines(out(client, client + "_PSR_manjob1.txt"))).containsExactly(
                "PSR|" + client + "|manjob1", "TX|E2EMJ1|ACSC", "END|1");
        assertThat(jdbc.queryForObject("SELECT type FROM prg_report WHERE file_name = ?",
                String.class, client + "_PSR_manjob1.txt")).isEqualTo("MANUAL");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM prg_delivery_ledger WHERE client = ?"
                + " AND manual_ref = 'OPS-9'", Long.class, client)).isEqualTo(1L);
    }

    // --- (8) kill-resume: ledger slice committed, no file yet -> resume re-renders the ledgered line ---

    @Test
    void killedMidImmediateReportResumeDeliversPreCrashLedgeredLines() throws Exception {
        String client = "FNBT10";
        cleanExchange(client, client + "_PSR_immcr-1.txt");
        UUID arrival = parent(client, "MSGCR");
        tx(arrival, 1, "E2ECR1", true);
        tx(arrival, 2, "E2ECR2", true);
        UUID b = batch(group(arrival, client, "MSGCR", 1), arrival, 1, "MSGCR");
        member(b, 1, "E2ECR1");
        member(b, 2, "E2ECR2");
        resp("pbsr_resp", b, "E2ECR1", "ACSC");
        resp("pbsr_resp", b, "E2ECR2", "RJCT");
        // post-SIGKILL state: report row + slice-1 ledger row committed (REQUIRES_NEW slices
        // land before the file's ATOMIC_MOVE), file absent, slice-2 row still unledgered
        UUID reportId = UUID.randomUUID();
        jdbc.update("INSERT INTO prg_report (id, client, type, trigger_kind, window_key,"
                        + " parent_source_msg_id, file_name) VALUES (?,?,?,?,?,?,?)",
                reportId, client, "IMMEDIATE", "COMPLETE", "immcr-1", "MSGCR",
                client + "_PSR_immcr-1.txt");
        jdbc.update("INSERT INTO prg_delivery_ledger (report_id, client, e2e, status)"
                + " VALUES (?,?,?,?)", reportId, client, "E2ECR1", "ACSC");

        var resumed = immediate.reportParent(client, "MSGCR", "immcr-1");

        assertThat(resumed).isPresent();
        // the pre-crash ledgered E2ECR1 line MUST re-render: ledger + watermark mark it
        // delivered, so no other path (scheduled delta, prg_report_due) will ever carry it
        assertThat(Files.readAllLines(resumed.get())).containsExactly(
                "PSR|" + client + "|immcr-1", "TX|E2ECR1|ACSC", "TX|E2ECR2|RJCT", "END|2");
        assertThat(autoLedgerRows(client)).containsExactly(
                new LedgerRow("E2ECR1", "ACSC"), new LedgerRow("E2ECR2", "RJCT"));
        assertThat(jdbc.queryForObject("SELECT count(*) FROM prg_watermark WHERE client = ?",
                Long.class, client)).isEqualTo(2L);
    }

    // --- (9) kill-resume: ALL slices ledgered, killed before the move -> resume still emits the file ---

    @Test
    void killedAfterAllSlicesLedgeredStillDeliversTheFileOnResume() throws Exception {
        String client = "FNBT11";
        cleanExchange(client, client + "_PSR_immca-1.txt");
        UUID arrival = parent(client, "MSGCA");
        tx(arrival, 1, "E2ECA1", true);
        UUID b = batch(group(arrival, client, "MSGCA", 1), arrival, 1, "MSGCA");
        member(b, 1, "E2ECA1");
        resp("pbsr_resp", b, "E2ECA1", "ACSC");
        UUID reportId = UUID.randomUUID();
        jdbc.update("INSERT INTO prg_report (id, client, type, trigger_kind, window_key,"
                        + " parent_source_msg_id, file_name) VALUES (?,?,?,?,?,?,?)",
                reportId, client, "IMMEDIATE", "COMPLETE", "immca-1", "MSGCA",
                client + "_PSR_immca-1.txt");
        jdbc.update("INSERT INTO prg_delivery_ledger (report_id, client, e2e, status)"
                + " VALUES (?,?,?,?)", reportId, client, "E2ECA1", "ACSC");

        var resumed = immediate.reportParent(client, "MSGCA", "immca-1");

        assertThat(resumed).isPresent();
        assertThat(Files.readAllLines(resumed.get())).containsExactly(
                "PSR|" + client + "|immca-1", "TX|E2ECA1|ACSC", "END|1");
        assertThat(jdbc.queryForObject("SELECT last_status FROM prg_watermark WHERE client = ?"
                + " AND e2e = 'E2ECA1'", String.class, client)).isEqualTo("ACSC");
    }

    // --- (5) negatives: unknown report.type fails the job; unknown report.id rejects the replay ---

    @Test
    void unknownReportTypeFailsTheJobAndUnknownReportIdRejectsReplay() throws Exception {
        JobParameters params = new JobParametersBuilder()
                .addString("client", "FNBT03", true)
                .addString("window", "neg1", true)
                .addString("report.type", "BOGUS", false)
                .toJobParameters();

        JobExecution failed = jobOperator.start(prgJob, params);

        assertThat(failed.getStatus()).isEqualTo(BatchStatus.FAILED);
        assertThat(failed.getAllFailureExceptions())
                .anyMatch(t -> t instanceof IllegalArgumentException && t.getMessage().contains("BOGUS"));

        UUID unknown = UUID.randomUUID();
        assertThatThrownBy(() -> immediate.replay(unknown))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(unknown.toString());
    }
}
