package za.co.fnb.dcre.prg.data;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.CockroachContainer;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

/**
 * SCRUM-55 perf regression gate for the reporting view stack (live chaos-gate
 * defect, 2026-07-16): with ONE parent of 12,000 tx the AGT trigger scan
 * (SELECT ... FROM prg_report_due) hung for 7+ minutes on the live single-node
 * CRDB, and ConcurrentExecution.SKIP then suppressed every later tick. The
 * view stack must answer from BOUNDED indexed paths: the whole-due scan AND a
 * full ext_tx_status scan each finish in under 5 seconds (generous vs the
 * minutes-scale hang, deterministic for CI) while still classifying both
 * seeded parents COMPLETE (all members terminal, nothing ledgered).
 *
 * <p>Payments changes only WHAT the stack joins on, never the shape it must
 * keep: prg_report_due binds its last_response arm through
 * prw_emission.outbound_msg_id rather than CRG's emission_id column, so the
 * regression this gate exists to catch is now a per-parent correlated scan of
 * THAT join: a set-based plan absorbs the wider join, an accidentally
 * correlated one does not. The fixture keeps the live incident's scale
 * (12,000 tx in one parent, 100 in a second) and the 5s bound unchanged.
 */
@SpringBootTest(properties = {
        "spring.liquibase.change-log=classpath:db/changelog/db.changelog-test-master.xml",
        "spring.batch.job.enabled=false", "dcre.exchange-root=build/test-exchange",
        "DCRE_EXCHANGE_ROOT=build/test-exchange"})
class ReportDuePerfIT {

    static final CockroachContainer CRDB =
            new CockroachContainer(DockerImageName.parse("cockroachdb/cockroach:v26.2.3"));

    static {
        CRDB.start();
    }

    @DynamicPropertySource
    static void props(final DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", CRDB::getJdbcUrl);
        registry.add("spring.datasource.username", CRDB::getUsername);
        registry.add("spring.datasource.password", CRDB::getPassword);
    }

    static final int BIG_PARENT_TX = 12_000;
    static final int SMALL_PARENT_TX = 100;
    static final int APPLIED_MAX = 5_000;
    static final long BUDGET_MS = 5_000L;

    @Autowired
    JdbcTemplate jdbc;

    @Test
    void reportDueAndFullExtScanAnswerWithinFiveSecondsOnATwelveThousandTxParent() {
        seedTerminalParent("FNBPF01", "MSGPERF1", BIG_PARENT_TX);
        seedTerminalParent("FNBPF01", "MSGPERF2", SMALL_PARENT_TX);
        final JdbcTemplate bounded = new JdbcTemplate(jdbc.getDataSource());
        bounded.setQueryTimeout((int) (BUDGET_MS / 1000)); // driver-side cancel: a hang reds out fast

        long start = System.nanoTime();
        final var due = bounded.queryForList(
                "SELECT client, source_msg_id, reason FROM prg_report_due"); // the AGT trigger scan, verbatim
        final long dueMs = elapsedMs(start);

        start = System.nanoTime();
        final Long extRows = bounded.queryForObject("SELECT count(*) FROM ext_tx_status", Long.class);
        final long extMs = elapsedMs(start);

        assertThat(due).extracting(r -> r.get("source_msg_id"), r -> r.get("reason"))
                .containsExactlyInAnyOrder(tuple("MSGPERF1", "COMPLETE"), tuple("MSGPERF2", "COMPLETE"));
        assertThat(extRows).isEqualTo(BIG_PARENT_TX + SMALL_PARENT_TX);
        assertThat(dueMs).as("prg_report_due full scan").isLessThan(BUDGET_MS);
        assertThat(extMs).as("ext_tx_status full scan").isLessThan(BUDGET_MS);
    }

    long elapsedMs(final long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000L;
    }

    /**
     * One parent at the live incident's shape: txCount entries, validated PASS,
     * split into APPLIED_MAX-sized batches, every member carrying an interim
     * ISR and a terminal PBSR response. The response cylinders carry NO
     * emission_id column here, so each reply is bound to its batch the only
     * way payments allows: orgnl_msg_id = that batch's outbound_msg_id.
     * Seeded set-based via generate_series: 12k-row loops of single INSERTs
     * would dominate the test's runtime.
     */
    void seedTerminalParent(final String client, final String msgId, final int txCount) {
        final UUID arrival = UUID.randomUUID();
        jdbc.update("INSERT INTO tx_header (arrival_id, msg_id_raw, msg_id, created_ts, tx_count,"
                        + " initg_pty, business_date, client_token, layout_version) VALUES (?,?,?,?,?,?,?,?,?)",
                arrival, msgId, msgId, "20260716080000", txCount, client, "20260716", client, 2);
        final int batches = (txCount + APPLIED_MAX - 1) / APPLIED_MAX;
        final UUID groupId = UUID.randomUUID();
        jdbc.update("INSERT INTO prw_emission_group (id, arrival_id, client, source_msg_id,"
                        + " applied_max, total_tx, total_amount, expected_batch_count, split)"
                        + " VALUES (?,?,?,?,?,?,?,?,?)",
                groupId, arrival, client, msgId, APPLIED_MAX, (long) txCount,
                BigDecimal.valueOf(txCount * 10L), batches, batches > 1);
        jdbc.update("INSERT INTO tx_entry (arrival_id, sequence, record_type, e2e_raw, e2e,"
                        + " creditor_account, currency, amount_raw, amount)"
                        + " SELECT ?, i, 'DC', ? || i, ? || i, '62000000010', 'ZAR', '1000', 10.00"
                        + " FROM generate_series(1, ?) AS s(i)",
                arrival, "E2E" + msgId + "-", "E2E" + msgId + "-", txCount);
        jdbc.update("INSERT INTO validation_log (arrival_id, sequence, outcome)"
                + " SELECT ?, i, 'PASS' FROM generate_series(1, ?) AS s(i)", arrival, txCount);
        for (int ordinal = 1; ordinal <= batches; ordinal++) {
            seedBatch(arrival, groupId, msgId, ordinal, txCount);
        }
    }

    /**
     * One outbound batch and its replies. outbound_msg_id is the batch's whole
     * correlation surface: the pick views, prg_member_status and the
     * prg_report_due last_response arm all reach these replies by joining
     * prw_emission.outbound_msg_id to orgnl_msg_id on each response table, so
     * a reply seeded with any other orgnl_msg_id would silently correlate to
     * nothing and the parent would never classify COMPLETE.
     */
    void seedBatch(final UUID arrival, final UUID groupId, final String msgId, final int ordinal,
                   final int txCount) {
        final int lo = (ordinal - 1) * APPLIED_MAX + 1;
        final int hi = Math.min(ordinal * APPLIED_MAX, txCount);
        final String outbound = msgId + "_" + ordinal;
        final UUID emissionId = UUID.randomUUID();
        jdbc.update("INSERT INTO prw_emission (id, arrival_id, file_name, state, group_id,"
                        + " batch_ordinal, outbound_msg_id, tx_count, visible_at)"
                        + " VALUES (?,?,?,'VISIBLE',?,?,?,?,now())",
                emissionId, arrival, outbound + "_PAIN008.xml", groupId, ordinal, outbound,
                (long) (hi - lo + 1));
        jdbc.update("INSERT INTO prw_emission_member (emission_id, sequence, e2e, amount)"
                        + " SELECT ?, i, ? || i, 10.00 FROM generate_series(?, ?) AS s(i)",
                emissionId, "E2E" + msgId + "-", lo, hi);
        jdbc.update("INSERT INTO isr_resp (response_file, orgnl_msg_id, e2e, status)"
                        + " SELECT ?, ?, ? || i, 'ACSP' FROM generate_series(?, ?) AS s(i)",
                "RESP_ISR_" + outbound + ".xml", outbound, "E2E" + msgId + "-", lo, hi);
        jdbc.update("INSERT INTO pbsr_resp (response_file, orgnl_msg_id, e2e, status)"
                        + " SELECT ?, ?, ? || i, 'ACSC' FROM generate_series(?, ?) AS s(i)",
                "RESP_PBSR_" + outbound + ".xml", outbound, "E2E" + msgId + "-", lo, hi);
    }
}
