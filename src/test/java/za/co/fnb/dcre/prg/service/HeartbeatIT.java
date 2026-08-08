package za.co.fnb.dcre.prg.service;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.CockroachContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SCRUM-55 Task 11 (spec test 8): zero-valued heartbeat PSR on quiet
 * SCHEDULED windows, so the OnHost consumer can tell "no movement" from
 * "PRG dead".
 * (1) a scheduled window with zero delta emits a normal-format PSR carrying
 *     exactly the HB zero line (SYNTHETIC-CONTRACT placeholder, DCRE + 29
 *     zeros, Max35-safe) + the PD pending count + END|0; the prg_report row
 *     is type HEARTBEAT; the watermark and the delivery ledger stay
 *     untouched; a restart of the same window is an R-24 no-op (same file,
 *     no duplicate registry row);
 * (2) PD counts the client's prg_sla_pending rows (VISIBLE batch members
 *     with a non-terminal status); an all-quiet book prints PD|0;
 * (3) a window WITH delta emits the normal file (TX lines, no HB/PD) with
 *     `type` SCHEDULED: the heartbeat never replaces a real report;
 * (4) kill-resume (review blocker crg-11): a same-window replay that finds a
 *     standing heartbeat but a late delta defers the delta to the next
 *     window; it never ledgers/advances against the zero-TX file, with the
 *     trailer END|0 classifying a registry-less heartbeat file.
 * Clients come from the FNBT isolation pool + FNBCC01 (one per test: report
 * file names and watermark rows are client-global).
 */
@SpringBootTest(properties = {"spring.batch.job.enabled=false", "dcre.exchange-root=build/test-exchange",
        "DCRE_EXCHANGE_ROOT=build/test-exchange"})
class HeartbeatIT {

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

    static final String HB_LINE =
            "HB|DCRE00000000000000000000000000000|DCRE00000000000000000000000000000|0|0.00";

    @Autowired
    JdbcTemplate jdbc;

    @Autowired
    PsrReportService service;

    // --- seed helpers (ImmediateReportIT shape; tables exist via the 001/003 bootstrap guards) ---

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

    /** No run_date: payments never warehouses, so it is not part of any emission identity. */
    UUID group(UUID arrival, String client, String msgId) {
        UUID id = UUID.randomUUID();
        jdbc.update("INSERT INTO prw_emission_group (id, arrival_id, client, source_msg_id,"
                        + " applied_max, total_tx, total_amount, expected_batch_count, split)"
                        + " VALUES (?,?,?,?,?,?,?,?,?)",
                id, arrival, client, msgId, 5000, 1L, 10.00, 1, false);
        return id;
    }

    /** VISIBLE with visible_at stamped: the prg_sla_pending precondition. */
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

    void watermark(String client, String e2e, String status) {
        jdbc.update("INSERT INTO prg_watermark (id, client, e2e, last_status)"
                + " VALUES (gen_random_uuid(), ?, ?, ?)", client, e2e, status);
    }

    long watermarkCount(String client) {
        Long count = jdbc.queryForObject("SELECT count(*) FROM prg_watermark WHERE client = ?",
                Long.class, client);
        return count == null ? -1 : count;
    }

    long ledgerCount(String client) {
        Long count = jdbc.queryForObject("SELECT count(*) FROM prg_delivery_ledger WHERE client = ?",
                Long.class, client);
        return count == null ? -1 : count;
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

    // --- (1) quiet scheduled window: exact zero-valued heartbeat, registry row, nothing advanced ---

    @Test
    void quietScheduledWindowEmitsZeroValuedHeartbeat() throws Exception {
        String client = "FNBCC01";
        cleanExchange(client, client + "_PSR_w123.txt");

        var path = service.window(client, "w123", false);

        assertThat(path).isPresent();
        assertThat(path.get()).isEqualTo(out(client, client + "_PSR_w123.txt"));
        assertThat(Files.readAllLines(path.get())).containsExactly(
                "PSR|FNBCC01|w123", HB_LINE, "PD|0", "END|0");
        assertThat(reportType(client + "_PSR_w123.txt")).isEqualTo("HEARTBEAT");
        // heartbeats never touch the watermark or the delivery ledger
        assertThat(watermarkCount(client)).isZero();
        assertThat(ledgerCount(client)).isZero();

        // restart of the SAME window: R-24 no-op, same file, no duplicate registry row
        var again = service.window(client, "w123", false);
        assertThat(again).contains(path.get());
        assertThat(jdbc.queryForObject("SELECT count(*) FROM prg_report WHERE file_name = ?",
                Long.class, client + "_PSR_w123.txt")).isEqualTo(1L);
    }

    // --- (2) PD line counts the client's prg_sla_pending rows; watermark stays untouched ---

    @Test
    void heartbeatPendingLineCountsSlaPendingRowsForTheClient() throws Exception {
        String client = "FNBT04";
        cleanExchange(client, client + "_PSR_hbpd-1.txt");
        UUID arrival = parent(client, "MSGHBPD");
        tx(arrival, 1, "E2EHBPD1");
        // PRW's outbound identity is the batch-suffixed form of the parent msg id, so a
        // correlation that binds here can only have bound through prw_emission.outbound_msg_id
        UUID b = batch(group(arrival, client, "MSGHBPD"), arrival, "MSGHBPD_1");
        member(b, 1, "E2EHBPD1");
        // current status CTV_PASS (non-terminal) already watermarked: zero delta, one SLA-pending row
        watermark(client, "E2EHBPD1", "CTV_PASS");

        var path = service.window(client, "hbpd-1", false);

        assertThat(path).isPresent();
        assertThat(Files.readAllLines(path.get())).containsExactly(
                "PSR|" + client + "|hbpd-1", HB_LINE, "PD|1", "END|0");
        assertThat(reportType(client + "_PSR_hbpd-1.txt")).isEqualTo("HEARTBEAT");
        // watermark table unchanged after emission: still exactly the seeded row
        assertThat(watermarkCount(client)).isEqualTo(1L);
        assertThat(jdbc.queryForObject("SELECT last_status FROM prg_watermark WHERE client = ?"
                + " AND e2e = 'E2EHBPD1'", String.class, client)).isEqualTo("CTV_PASS");
    }

    @Test
    void heartbeatPendingLineSuppressesWarehousedStatuses() throws Exception {
        // SCRUM-68 companion to the PD-count test: a member whose LATEST status
        // is accepted-warehoused (sla_suppressed, e.g. ACWC) holds its response
        // and must NOT age on the SLA path, so the emission-path heartbeat
        // prints PD|0 (pre-005 this exact seed printed PD|1).
        String client = "FNBT08";
        cleanExchange(client, client + "_PSR_hbsw-1.txt");
        UUID arrival = parent(client, "MSGHBSW");
        tx(arrival, 1, "E2EHBSW1");
        String outbound = "MSGHBSW_1";
        UUID b = batch(group(arrival, client, "MSGHBSW"), arrival, outbound);
        member(b, 1, "E2EHBSW1");
        // warehoused response already watermarked: zero delta, zero SLA-pending.
        // pbsr_resp carries NO emission_id column in dcre_pay: the reply reaches its batch
        // through orgnl_msg_id = prw_emission.outbound_msg_id, the one correlation path PRG has.
        jdbc.update("INSERT INTO pbsr_resp (response_file, orgnl_msg_id, e2e, status)"
                + " VALUES (?,?,?,?)", "RESP_HBSW_1.xml", outbound, "E2EHBSW1", "ACWC");
        watermark(client, "E2EHBSW1", "ACWC");

        var path = service.window(client, "hbsw-1", false);

        assertThat(path).isPresent();
        assertThat(Files.readAllLines(path.get())).containsExactly(
                "PSR|" + client + "|hbsw-1", HB_LINE, "PD|0", "END|0");
        assertThat(reportType(client + "_PSR_hbsw-1.txt")).isEqualTo("HEARTBEAT");
        // watermark untouched: still exactly the seeded ACWC row
        assertThat(watermarkCount(client)).isEqualTo(1L);
        assertThat(jdbc.queryForObject("SELECT last_status FROM prg_watermark WHERE client = ?"
                + " AND e2e = 'E2EHBSW1'", String.class, client)).isEqualTo("ACWC");
    }

    // --- (3) a window WITH delta emits the normal file: TX lines only, no HB/PD, type SCHEDULED ---

    @Test
    void windowWithDeltaEmitsTheNormalFileWithoutHeartbeatLines() throws Exception {
        String client = "FNBT05";
        cleanExchange(client, client + "_PSR_hbdl-1.txt");
        UUID arrival = parent(client, "MSGHBDL");
        tx(arrival, 1, "E2EHBDL1");

        var path = service.window(client, "hbdl-1", false);

        assertThat(path).isPresent();
        assertThat(Files.readAllLines(path.get())).containsExactly(
                "PSR|" + client + "|hbdl-1", "TX|E2EHBDL1|CTV_PASS", "END|1");
        assertThat(reportType(client + "_PSR_hbdl-1.txt")).isEqualTo("SCHEDULED");
    }

    // --- (4) kill-resume regression (review blocker crg-11): a standing heartbeat must never
    // absorb a delta that arrived between the kill and the same-window relaunch. Pre-fix the
    // replay skipped streamPsr (file exists) but still ledgered + watermark-advanced every
    // delta row against the HEARTBEAT report row: statuses never present in ANY emitted file
    // were recorded as externally delivered and permanently suppressed. ---

    @Test
    void sameWindowReplayAfterHeartbeatDefersTheLateDeltaToTheNextWindow() throws Exception {
        String client = "FNBT06";
        cleanExchange(client, client + "_PSR_hbkr-1.txt", client + "_PSR_hbkr-2.txt");

        // quiet window: the heartbeat stands
        var hb = service.window(client, "hbkr-1", false);
        assertThat(hb).isPresent();
        assertThat(Files.readAllLines(hb.get())).containsExactly(
                "PSR|" + client + "|hbkr-1", HB_LINE, "PD|0", "END|0");

        // a response arrives between the mid-window kill and the OrphanSweeper relaunch
        UUID arrival = parent(client, "MSGHBKR");
        tx(arrival, 1, "E2EHBKR1");

        // relaunch of the SAME (client, window) identity: heartbeat file untouched,
        // NOTHING ledgered or watermark-advanced (the terminal status must still reach
        // the client via the next window)
        var replay = service.window(client, "hbkr-1", false);
        assertThat(replay).contains(hb.get());
        assertThat(Files.readAllLines(hb.get())).containsExactly(
                "PSR|" + client + "|hbkr-1", HB_LINE, "PD|0", "END|0");
        assertThat(ledgerCount(client)).isZero();
        assertThat(watermarkCount(client)).isZero();
        assertThat(reportType(client + "_PSR_hbkr-1.txt")).isEqualTo("HEARTBEAT");

        // the deferred delta flows to the NEXT window as a normal scheduled report
        var next = service.window(client, "hbkr-2", false);
        assertThat(next).isPresent();
        assertThat(Files.readAllLines(next.get())).containsExactly(
                "PSR|" + client + "|hbkr-2", "TX|E2EHBKR1|CTV_PASS", "END|1");
        assertThat(reportType(client + "_PSR_hbkr-2.txt")).isEqualTo("SCHEDULED");
        assertThat(ledgerCount(client)).isEqualTo(1L);
        assertThat(watermarkCount(client)).isEqualTo(1L);
    }

    @Test
    void heartbeatFileWithoutRegistryRowStillDefersTheDeltaByItsTrailer() throws Exception {
        // crash in the narrow window between the heartbeat ATOMIC_MOVE and the registry
        // insert: the END|0 file stands with NO prg_report row. The replay must classify
        // it by its trailer (a real delta file always counts at least one TX line),
        // converge the registry to HEARTBEAT, and still defer the delta.
        String client = "FNBT07";
        cleanExchange(client, client + "_PSR_hbnr-1.txt");

        var hb = service.window(client, "hbnr-1", false);
        assertThat(hb).isPresent();
        jdbc.update("DELETE FROM prg_report WHERE file_name = ?", client + "_PSR_hbnr-1.txt");

        UUID arrival = parent(client, "MSGHBNR");
        tx(arrival, 1, "E2EHBNR1");

        var replay = service.window(client, "hbnr-1", false);
        assertThat(replay).contains(hb.get());
        assertThat(ledgerCount(client)).isZero();
        assertThat(watermarkCount(client)).isZero();
        assertThat(reportType(client + "_PSR_hbnr-1.txt")).isEqualTo("HEARTBEAT");
    }
}
