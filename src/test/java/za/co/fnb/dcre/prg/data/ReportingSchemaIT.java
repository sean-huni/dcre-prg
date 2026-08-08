package za.co.fnb.dcre.prg.data;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.CockroachContainer;
import org.testcontainers.utility.DockerImageName;
import za.co.fnb.dcre.prg.data.model.LedgerRow;
import za.co.fnb.dcre.prg.data.model.PrgReportEntity;
import za.co.fnb.dcre.prg.data.model.StatusRow;
import za.co.fnb.dcre.prg.data.repo.PrgDeliveryLedgerRepo;
import za.co.fnb.dcre.prg.data.repo.PrgReportRepo;
import za.co.fnb.dcre.prg.data.repo.PrgWatermarkRepo;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

/**
 * PRG schema contract over dcre_pay (the deliberately-unfrozen prg_report_due
 * SQL is DEFINED by these cases):
 * (a) ext_tx_status is batch-scoped: an identical e2e under a different arrival
 *     never cross-links a response (A-40 guard). The payments response cylinders
 *     carry NO emission_id column (PIX/PSX/PPX dropped CIX's emission FK), so a
 *     reply reaches its batch by ONE path, the globally unique outbound identity
 *     (orgnl_msg_id = prw_emission.outbound_msg_id, uq_prw_emission_outbound_msg);
 *     an arrival with NO prw_emission row at all falls back to its parent
 *     identity family (orgnl_msg_id = parent MsgId or a split child MsgId_N).
 *     Per response table only the LATEST row per (emission, e2e) projects (max
 *     created_at, response_file tiebreaker), so a resend or a second response
 *     file for one emission never multiplies rows or flip-flops the status pick;
 *     prg_sla_pending uses the corresponding member-grain picks.
 * (b) prg_delivery_ledger auto rows are DB-arbitrated once per
 *     (client, e2e, status) by the partial index uq_prg_ledger_auto; manual_ref
 *     rows fall outside it, so they bypass the guard and still audit.
 * (c) prg_report_due lists COMPLETE (all members terminal, immediately) and
 *     IDLE (responses quiet past the 120s debounce with unreported deltas);
 *     recent, fully-ledgered and zero-response parents are absent.
 * (d) prg_status_class is fully classified at its v1 baseline: fourteen codes,
 *     no UNSUPPORTED row and no historical-correction step. ACWC/ACWP are
 *     accepted warehoused interim statuses per the RMB DebiCheck profile
 *     (ACWP future-dated, ACWC auto-bumped): reportable, non-terminal and
 *     suppressed from the SLA aging path. ENDO cannot warehouse, so payments
 *     does not expect them; they are catalogued anyway because an unlisted code
 *     that did arrive would be silently suppressed as unknown. Unrecognised
 *     codes still fail closed as non-reportable protocol exceptions.
 * (e) prg_sla_pending ages non-terminal members of VISIBLE batches.
 */
@SpringBootTest(properties = {"spring.batch.job.enabled=false", "dcre.exchange-root=build/test-exchange",
        "DCRE_EXCHANGE_ROOT=build/test-exchange"})
class ReportingSchemaIT {

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

    @Autowired
    JdbcTemplate jdbc;

    @Autowired
    PrgReportRepo reports;

    @Autowired
    PrgDeliveryLedgerRepo ledger;

    @Autowired
    PrgWatermarkRepo watermarks;

    record ExtRow(String status, boolean terminal, UUID emissionId, String outboundMsgId, String sourceMsgId) {
    }

    record StatusClass(String classification, boolean terminal, boolean reportable,
                       boolean slaSuppressed) {
    }

    // --- seed helpers (tables exist via the 001/003 bootstrap guards) ---

    UUID parent(final String client, final String msgId) {
        UUID arrival = UUID.randomUUID();
        jdbc.update("INSERT INTO tx_header (arrival_id, msg_id_raw, msg_id, created_ts, tx_count,"
                        + " initg_pty, business_date, client_token, layout_version) VALUES (?,?,?,?,?,?,?,?,?)",
                arrival, msgId, msgId, "20260716080000", 1, client, "20260716", client, 2);
        return arrival;
    }

    void tx(final UUID arrival, final int seq, final String e2e) {
        jdbc.update("INSERT INTO tx_entry (arrival_id, sequence, record_type, e2e_raw, e2e,"
                        + " creditor_account, currency, amount_raw, amount) VALUES (?,?,?,?,?,?,?,?,?)",
                arrival, seq, "DC", e2e, e2e, "62000000010", "ZAR", "1000", 10.00);
        jdbc.update("INSERT INTO validation_log (arrival_id, sequence, outcome) VALUES (?,?,'PASS')", arrival, seq);
    }

    /** No run_date: payments never warehouses, so it is part of no PRW identity. */
    UUID group(final UUID arrival, final String client, final String msgId, final int batches) {
        UUID id = UUID.randomUUID();
        jdbc.update("INSERT INTO prw_emission_group (id, arrival_id, client, source_msg_id,"
                        + " applied_max, total_tx, total_amount, expected_batch_count, split)"
                        + " VALUES (?,?,?,?,?,?,?,?,?)",
                id, arrival, client, msgId, 5000, 2L, 20.00, batches, batches > 1);
        return id;
    }

    /** group_id, outbound_msg_id and file_name are NOT NULL on prw_emission (CRW allowed nulls). */
    UUID batch(final UUID groupId, final UUID arrival, final int ordinal, final String outboundMsgId) {
        UUID id = UUID.randomUUID();
        jdbc.update("INSERT INTO prw_emission (id, group_id, arrival_id, batch_ordinal,"
                        + " outbound_msg_id, file_name, state, visible_at) VALUES (?,?,?,?,?,?,?,now())",
                id, groupId, arrival, ordinal, outboundMsgId, outboundMsgId + "_PAIN008.xml", "VISIBLE");
        return id;
    }

    void member(final UUID emissionId, final int seq, final String e2e) {
        jdbc.update("INSERT INTO prw_emission_member (emission_id, sequence, e2e, amount) VALUES (?,?,?,?)",
                emissionId, seq, e2e, 10.00);
    }

    /**
     * Correlated reply: the response tables have no emission_id column, so
     * "this reply belongs to that batch" is expressed the only way PRG can read
     * it, orgnl_msg_id = the batch's globally unique outbound_msg_id.
     * agedSeconds null = fresh response; else updated_at is backdated past the
     * 120s debounce.
     */
    void resp(final String table, final UUID emissionId, final String e2e, final String status,
              final Integer agedSeconds) {
        resp(table, outboundOf(emissionId), e2e, status, agedSeconds);
    }

    /**
     * Raw-identity variant: a reply naming a parent MsgId (or a split child
     * MsgId_N) rather than a batch outbound, which is how a no-emission arrival
     * reaches its transactions through the family fallback.
     */
    void resp(final String table, final String orgnlMsgId, final String e2e, final String status,
              final Integer agedSeconds) {
        jdbc.update("INSERT INTO " + table + " (response_file, orgnl_msg_id, e2e, status)"
                        + " VALUES (?,?,?,?)",
                "RESP_%s_%s_%s.xml".formatted(table, e2e, orgnlMsgId), orgnlMsgId, e2e, status);
        if (agedSeconds != null) {
            jdbc.update("UPDATE " + table + " SET updated_at = now() - INTERVAL '" + agedSeconds
                    + " seconds' WHERE e2e = ?", e2e);
        }
    }

    /**
     * Latest-row fixture seed: explicit response_file plus a backdated
     * created_at (and updated_at, for the due-view debounce) so resend twins of
     * one (emission, e2e) carry a deterministic recency order.
     */
    void respAt(final String table, final UUID emissionId, final String e2e, final String status,
                final String responseFile, final int createdAgoSeconds) {
        respAt(table, outboundOf(emissionId), e2e, status, responseFile, createdAgoSeconds);
    }

    void respAt(final String table, final String orgnlMsgId, final String e2e, final String status,
                final String responseFile, final int createdAgoSeconds) {
        jdbc.update("INSERT INTO " + table + " (response_file, orgnl_msg_id, e2e, status)"
                + " VALUES (?,?,?,?)", responseFile, orgnlMsgId, e2e, status);
        jdbc.update("UPDATE " + table + " SET created_at = now() - INTERVAL '" + createdAgoSeconds
                + " seconds', updated_at = now() - INTERVAL '" + createdAgoSeconds
                + " seconds' WHERE response_file = ?", responseFile);
    }

    /** The one correlation key PRG has; unique per uq_prw_emission_outbound_msg. */
    String outboundOf(final UUID emissionId) {
        return jdbc.queryForObject("SELECT outbound_msg_id FROM prw_emission WHERE id = ?",
                String.class, emissionId);
    }

    ExtRow ext(final UUID arrival, final String e2e) {
        return jdbc.queryForObject("SELECT status, terminal, emission_id, outbound_msg_id, source_msg_id"
                        + " FROM ext_tx_status WHERE arrival_id = ? AND e2e = ?",
                (rs, i) -> new ExtRow(rs.getString("status"), rs.getBoolean("terminal"),
                        (UUID) rs.getObject("emission_id"), rs.getString("outbound_msg_id"),
                        rs.getString("source_msg_id")), arrival, e2e);
    }

    long extCount(final UUID arrival, final String e2e) {
        Long count = jdbc.queryForObject("SELECT count(*) FROM ext_tx_status WHERE arrival_id = ?"
                + " AND e2e = ?", Long.class, arrival, e2e);
        return count == null ? -1 : count;
    }

    long slaCount(final String client) {
        Long count = jdbc.queryForObject("SELECT count(*) FROM prg_sla_pending WHERE client = ?",
                Long.class, client);
        return count == null ? -1 : count;
    }

    Map<String, String> due(final String client) {
        Map<String, String> rows = new HashMap<>();
        jdbc.query("SELECT source_msg_id, reason FROM prg_report_due WHERE client = ?",
                rs -> { rows.put(rs.getString("source_msg_id"), rs.getString("reason")); }, client);
        return rows;
    }

    // --- (a) batch-scoped status projection ---

    @Test
    void extTxStatusNeverCrossLinksIdenticalE2eAcrossArrivals() {
        UUID a1 = parent("FNBRF01", "MSGA1");
        tx(a1, 1, "E2EDUP");
        UUID b1 = batch(group(a1, "FNBRF01", "MSGA1", 1), a1, 1, "MSGA1");
        member(b1, 1, "E2EDUP");
        UUID a2 = parent("FNBRF01", "MSGA2");
        tx(a2, 1, "E2EDUP");
        UUID b2 = batch(group(a2, "FNBRF01", "MSGA2", 1), a2, 1, "MSGA2");
        member(b2, 1, "E2EDUP");
        resp("pbsr_resp", b1, "E2EDUP", "ACSC", null); // arrival-1's batch ONLY

        ExtRow hit = ext(a1, "E2EDUP");
        assertThat(hit.status()).isEqualTo("ACSC");
        assertThat(hit.terminal()).isTrue();
        assertThat(hit.emissionId()).isEqualTo(b1);
        assertThat(hit.outboundMsgId()).isEqualTo("MSGA1");
        assertThat(hit.sourceMsgId()).isEqualTo("MSGA1");

        ExtRow other = ext(a2, "E2EDUP"); // A-40 guard: no cross-link
        assertThat(other.status()).isEqualTo("CTV_PASS");
        assertThat(other.terminal()).isFalse();
    }

    @Test
    void noEmissionArrivalProjectsThroughItsParentIdentityFamily() {
        UUID a3 = parent("FNBRF01", "MSGA3");
        tx(a3, 1, "E2ELEG");
        resp("isr_resp", "MSGA3", "E2ELEG", "ACSP", null); // no registry row to bind to

        assertThat(ext(a3, "E2ELEG").status()).isEqualTo("ACSP");

        UUID a4 = parent("FNBRF01", "MSGA4"); // split child reply, still no registry row
        tx(a4, 1, "E2ELEG2");
        resp("isr_resp", "MSGA4_2", "E2ELEG2", "ACSP", null);

        assertThat(ext(a4, "E2ELEG2").status()).isEqualTo("ACSP");
    }

    @Test
    void noEmissionArrivalWarehousedStatusProjectsAsReportable() {
        // prg_status_class is seeded at v1 with ACWC already classified
        // accepted-warehoused, so a warehoused code projects as a reportable
        // interim status rather than as a protocol exception.
        String client = "LEG" + UUID.randomUUID().toString().substring(0, 8);
        UUID arrival = parent(client, "MSGLEGACYEX");
        tx(arrival, 1, "ELEGACYEX");
        resp("pbsr_resp", "MSGLEGACYEX", "ELEGACYEX", "ACWC", 300);

        ExtRow row = ext(arrival, "ELEGACYEX");
        assertThat(row.status()).isEqualTo("ACWC");
        assertThat(row.terminal()).isFalse();
        // Family-binding proof: a no-emission arrival carries no batch identity
        // in ext_tx_status (outbound/source/emission all come from the
        // member_emission -> group join). The header msg_id fallback is an
        // exception-view COALESCE concern, proven by the unknown-code twin below.
        assertThat(row.emissionId()).isNull();
        assertThat(row.outboundMsgId()).isNull();
        assertThat(row.sourceMsgId()).isNull();
        assertThat(jdbc.queryForObject(
                "SELECT count(*) FROM prg_status_exception WHERE client=? AND e2e='ELEGACYEX'",
                Long.class, client)).isZero();
        assertThat(watermarks.findRangeSlice(client, "", 10))
                .containsExactly(new StatusRow("ELEGACYEX", "ACWC"));
    }

    @Test
    void noEmissionArrivalUnknownStatusRemainsAProtocolExceptionViaTheHeaderFallback() {
        // Pins the prg_status_exception COALESCE arm: a family-bound row on a
        // no-emission arrival surfaces with source_msg_id falling back to the
        // header msg_id and outbound_msg_id NULL.
        String client = "LGU" + UUID.randomUUID().toString().substring(0, 8);
        UUID arrival = parent(client, "MSGLEGUNK");
        tx(arrival, 1, "ELEGUNK");
        resp("pbsr_resp", "MSGLEGUNK", "ELEGUNK", "ZZZZ", 300);

        ExtRow row = ext(arrival, "ELEGUNK");
        assertThat(row.status()).isEqualTo("ZZZZ");
        assertThat(row.terminal()).isFalse();
        assertThat(jdbc.queryForMap(
                "SELECT source_msg_id, outbound_msg_id, status, classification"
                        + " FROM prg_status_exception WHERE client=? AND e2e='ELEGUNK'", client))
                .containsEntry("source_msg_id", "MSGLEGUNK")
                .containsEntry("outbound_msg_id", null)
                .containsEntry("status", "ZZZZ")
                .containsEntry("classification", "UNKNOWN");
        assertThat(watermarks.findRangeSlice(client, "", 10)).isEmpty();
    }

    @Test
    void batchBoundAndFamilyBoundRepliesOfOneE2eNeverCrossLink() {
        // A-40 guard shape: the SAME e2e in TWO arrivals; arrival-1 has a batch,
        // arrival-2 has no emission at all, so the two replies must reach their
        // transactions by DIFFERENT paths and neither may reach the other's.
        UUID a1 = parent("FNBRF04", "MSGN1");
        tx(a1, 1, "E2ENUL");
        UUID b1 = batch(group(a1, "FNBRF04", "MSGN1", 1), a1, 1, "MSGN1");
        member(b1, 1, "E2ENUL");
        UUID a2 = parent("FNBRF04", "MSGN2");
        tx(a2, 1, "E2ENUL"); // deliberately NO emission
        respAt("pbsr_resp", b1, "E2ENUL", "ACSC", "RESP_NUL_P1.xml", 0); // names batch-1's outbound

        // The reply names batch-1's globally unique outbound, so it projects
        // THERE and never onto parent-2, whose family it does not name.
        assertThat(ext(a1, "E2ENUL").status()).isEqualTo("ACSC");
        assertThat(ext(a1, "E2ENUL").terminal()).isTrue();
        assertThat(ext(a2, "E2ENUL").status()).isEqualTo("CTV_PASS");

        respAt("pbsr_resp", "MSGN2", "E2ENUL", "ACSP", "RESP_NUL_P2.xml", 0); // names parent-2

        assertThat(ext(a2, "E2ENUL").status()).isEqualTo("ACSP"); // no emission at all: fail-open truth
        assertThat(extCount(a1, "E2ENUL")).isEqualTo(1L);
        assertThat(extCount(a2, "E2ENUL")).isEqualTo(1L);
    }

    @Test
    void secondResponseFileForTheSameEmissionProjectsOnlyTheNewestStatus() {
        String client = "FNBRF06";
        UUID a = parent(client, "MSGRS1");
        tx(a, 1, "E2ERSND");
        UUID b = batch(group(a, client, "MSGRS1", 1), a, 1, "MSGRS1");
        member(b, 1, "E2ERSND");
        respAt("pbsr_resp", b, "E2ERSND", "ACSP", "RESP_RSND_FIRST.xml", 300);

        assertThat(ext(a, "E2ERSND").status()).isEqualTo("ACSP");
        assertThat(slaCount(client)).isEqualTo(1L); // interim member of a VISIBLE batch ages

        // resend: a SECOND response file arrives for the SAME (emission, e2e)
        respAt("pbsr_resp", b, "E2ERSND", "ACSC", "RESP_RSND_SECOND.xml", 0);

        assertThat(extCount(a, "E2ERSND")).isEqualTo(1L); // no row multiplication
        assertThat(ext(a, "E2ERSND").status()).isEqualTo("ACSC"); // newest created_at wins
        assertThat(ext(a, "E2ERSND").terminal()).isTrue();
        assertThat(slaCount(client)).isZero(); // the stale interim row cannot resurrect the member
    }

    @Test
    void equalTimestampResponseTwinsTieBreakDeterministicallyOnResponseFile() {
        String client = "FNBRF07";
        UUID a = parent(client, "MSGTB1");
        tx(a, 1, "E2ETIE");
        UUID b = batch(group(a, client, "MSGTB1", 1), a, 1, "MSGTB1");
        member(b, 1, "E2ETIE");
        respAt("isr_resp", b, "E2ETIE", "ACSP", "RESP_TIE_A.xml", 0);
        respAt("isr_resp", b, "E2ETIE", "ACTC", "RESP_TIE_B.xml", 0);
        jdbc.update("UPDATE isr_resp SET created_at = '2026-07-16 08:00:00+00' WHERE e2e = 'E2ETIE'");

        assertThat(extCount(a, "E2ETIE")).isEqualTo(1L);
        assertThat(ext(a, "E2ETIE").status()).isEqualTo("ACTC"); // greater response_file wins the tie
    }

    // --- (b) report registry + ledger auto-guard + manual bypass ---

    @Test
    void ledgerAutoRowsInsertOncePerClientE2eStatusButManualBypasses() {
        PrgReportEntity first = reports.save(PrgReportEntity.of(
                "FNBCC09", "IMMEDIATE", "COMPLETE", "imm-1", "MSGB", "FNBCC09_PSR_imm-1.txt"));
        ledger.record(first.getId(), "FNBCC09", "E2EB1", "ACSC", null);
        ledger.record(first.getId(), "FNBCC09", "E2EB1", "ACSC", null); // same tuple: silent no-op
        assertThat(ledger.countForReport(first.getId())).isEqualTo(1);

        PrgReportEntity second = reports.save(PrgReportEntity.of(
                "FNBCC09", "IMMEDIATE", "IDLE", "imm-2", "MSGB", "FNBCC09_PSR_imm-2.txt"));
        ledger.record(second.getId(), "FNBCC09", "E2EB1", "ACSC", null); // cross-report auto dedupe
        assertThat(ledger.countForReport(second.getId())).isZero();

        ledger.record(second.getId(), "FNBCC09", "E2EB1", "ACSC", "OPS-1"); // manual: bypass, still audited
        assertThat(ledger.countForReport(second.getId())).isEqualTo(1);
        assertThat(ledger.rowsForReport(second.getId())).containsExactly(new LedgerRow("E2EB1", "ACSC"));
        assertThat(jdbc.queryForObject("SELECT count(*) FROM prg_delivery_ledger WHERE client='FNBCC09'"
                + " AND e2e='E2EB1' AND status='ACSC' AND manual_ref IS NULL", Long.class)).isEqualTo(1L);

        assertThat(reports.findByClientAndParentSourceMsgId("FNBCC09", "MSGB")).hasSize(2);
        assertThat(reports.findById(first.getId())).isPresent();
        // The registry column is `type`, not CRG's report_type: a column never
        // repeats its own table name, and a v1 baseline is the only free moment.
        assertThat(jdbc.queryForObject("SELECT type FROM prg_report WHERE id = ?", String.class,
                first.getId())).isEqualTo("IMMEDIATE");
    }

    // --- (c) the due-view contract ---

    @Test
    void reportDueListsCompleteImmediatelyAndIdleAfterDebounceOnly() {
        String client = "FNBCC01";

        UUID ac = parent(client, "MSGC"); // COMPLETE: every member terminal, responses FRESH
        tx(ac, 1, "EC1");
        tx(ac, 2, "EC2");
        UUID bc = batch(group(ac, client, "MSGC", 1), ac, 1, "MSGC");
        member(bc, 1, "EC1");
        member(bc, 2, "EC2");
        resp("pbsr_resp", bc, "EC1", "ACSC", null);
        resp("pbsr_resp", bc, "EC2", "RJCT", null);

        UUID ai = parent(client, "MSGI"); // IDLE: interim response aged past 120s, EI2 still pending
        tx(ai, 1, "EI1");
        tx(ai, 2, "EI2");
        UUID bi = batch(group(ai, client, "MSGI", 1), ai, 1, "MSGI");
        member(bi, 1, "EI1");
        member(bi, 2, "EI2");
        resp("isr_resp", bi, "EI1", "ACSP", 300);

        UUID ar = parent(client, "MSGR"); // recent response: debounce still running, NOT due
        tx(ar, 1, "ER1");
        UUID br = batch(group(ar, client, "MSGR", 1), ar, 1, "MSGR");
        member(br, 1, "ER1");
        resp("isr_resp", br, "ER1", "ACSP", null);

        UUID al = parent(client, "MSGL"); // complete but fully ledgered: nothing unreported, NOT due
        tx(al, 1, "EL1");
        UUID bl = batch(group(al, client, "MSGL", 1), al, 1, "MSGL");
        member(bl, 1, "EL1");
        resp("pbsr_resp", bl, "EL1", "ACSC", null);
        PrgReportEntity done = reports.save(PrgReportEntity.of(
                client, "IMMEDIATE", "COMPLETE", "imm-c", "MSGL", client + "_PSR_imm-c.txt"));
        ledger.record(done.getId(), client, "EL1", "ACSC", null);

        UUID az = parent(client, "MSGZ"); // zero responses: debounce never started, NOT due
        tx(az, 1, "EZ1");
        UUID bz = batch(group(az, client, "MSGZ", 1), az, 1, "MSGZ");
        member(bz, 1, "EZ1");

        assertThat(due(client)).containsOnly(entry("MSGC", "COMPLETE"), entry("MSGI", "IDLE"));
    }

    @Test
    void partialEmissionParentWithAllPresentMembersTerminalClassifiesIdleNeverComplete() {
        // Boundary pin: the group's helper total_tx is 2, but only ONE member was
        // ever emitted (partial emission: the second tx never made a batch).
        // Every PRESENT member is terminal and the responses aged past the 120s
        // debounce; completeness is judged against total_tx, so the parent must
        // classify IDLE and never COMPLETE.
        String client = "FNBCC02";
        UUID ap = parent(client, "MSGPART");
        tx(ap, 1, "EPB1");
        tx(ap, 2, "EPB2");
        UUID bp = batch(group(ap, client, "MSGPART", 1), ap, 1, "MSGPART");
        member(bp, 1, "EPB1"); // EPB2 deliberately never emitted
        resp("pbsr_resp", bp, "EPB1", "ACSC", 300); // terminal, aged past debounce

        assertThat(due(client)).containsOnly(entry("MSGPART", "IDLE"));
    }

    // --- (d) warehoused interim codes report; unknown codes fail closed ---

    @Test
    void warehousedStatusesAreReportableAndSlaSuppressed() {
        UUID au = parent("FNBRF02", "MSGU");
        tx(au, 1, "EU1");
        tx(au, 2, "EU2");
        UUID bu = batch(group(au, "FNBRF02", "MSGU", 1), au, 1, "MSGU");
        member(bu, 1, "EU1");
        member(bu, 2, "EU2");
        resp("pbsr_resp", bu, "EU1", "ACWC", 300);
        resp("pbsr_resp", bu, "EU2", "ACWP", 300);
        jdbc.update("UPDATE prw_emission SET visible_at = now() - INTERVAL '21 hours' WHERE id = ?", bu);

        ExtRow row = ext(au, "EU1");
        assertThat(row.status()).isEqualTo("ACWC");
        assertThat(row.terminal()).isFalse();
        // Reportable interim rows past the 120s debounce make the parent due.
        assertThat(due("FNBRF02")).containsOnly(entry("MSGU", "IDLE"));
        // The warehoused tx holds its response: it must NOT age on the SLA path.
        assertThat(slaCount("FNBRF02")).isZero();

        // The WHOLE v1 catalogue: fourteen codes, every one classified, and no
        // UNSUPPORTED row for an unrecognised code to land on.
        Map<String, StatusClass> expected = Map.ofEntries(
                Map.entry("ACSC", new StatusClass("TERMINAL_SUCCESS", true, true, false)),
                Map.entry("ACCC", new StatusClass("TERMINAL_SUCCESS", true, true, false)),
                Map.entry("RJCT", new StatusClass("TERMINAL_NON_SUCCESS", true, true, false)),
                Map.entry("CANC", new StatusClass("TERMINAL_NON_SUCCESS", true, true, false)),
                Map.entry("ACSP", new StatusClass("ACCEPTED_NON_TERMINAL", false, true, false)),
                Map.entry("ACTC", new StatusClass("ACCEPTED_NON_TERMINAL", false, true, false)),
                Map.entry("ACCP", new StatusClass("ACCEPTED_NON_TERMINAL", false, true, false)),
                Map.entry("ACFC", new StatusClass("ACCEPTED_NON_TERMINAL", false, true, false)),
                Map.entry("RCVD", new StatusClass("PENDING_INTERIM", false, true, false)),
                Map.entry("PDNG", new StatusClass("PENDING_INTERIM", false, true, false)),
                Map.entry("PART", new StatusClass("PENDING_INTERIM", false, true, false)),
                Map.entry("PATC", new StatusClass("PENDING_INTERIM", false, true, false)),
                Map.entry("ACWC", new StatusClass("ACCEPTED_NON_TERMINAL", false, true, true)),
                Map.entry("ACWP", new StatusClass("ACCEPTED_NON_TERMINAL", false, true, true)));
        Map<String, StatusClass> actual = jdbc.query(
                "SELECT code, classification, terminal, reportable, sla_suppressed FROM prg_status_class",
                result -> {
                    Map<String, StatusClass> classes = new HashMap<>();
                    while (result.next()) {
                        classes.put(result.getString("code"), new StatusClass(
                                result.getString("classification"), result.getBoolean("terminal"),
                                result.getBoolean("reportable"), result.getBoolean("sla_suppressed")));
                    }
                    return classes;
                });
        assertThat(actual).containsExactlyInAnyOrderEntriesOf(expected);

        assertThat(jdbc.queryForObject(
                "SELECT count(*) FROM prg_status_exception WHERE client='FNBRF02'", Long.class)).isZero();
        assertThat(watermarks.findDeltaSlice("FNBRF02", "", 10))
                .containsExactly(new StatusRow("EU1", "ACWC"), new StatusRow("EU2", "ACWP"));
        assertThat(watermarks.findRangeSlice("FNBRF02", "", 10))
                .containsExactly(new StatusRow("EU1", "ACWC"), new StatusRow("EU2", "ACWP"));
        assertThat(watermarks.findUnreportedForParent("FNBRF02", "MSGU", 10))
                .containsExactly(new StatusRow("EU1", "ACWC"), new StatusRow("EU2", "ACWP"));
        assertThat(watermarks.findCurrentForParent("FNBRF02", "MSGU", "", 10))
                .containsExactly(new StatusRow("EU1", "ACWC"), new StatusRow("EU2", "ACWP"));
    }

    @Test
    void unknownFintegrateStatusIsPreservedAsNonReportableProtocolException() {
        String client = "UNK" + UUID.randomUUID().toString().substring(0, 8);
        UUID arrival = parent(client, "MSGUNKNOWN");
        tx(arrival, 1, "EUNKNOWN");
        UUID emission = batch(group(arrival, client, "MSGUNKNOWN", 1), arrival, 1, "MSGUNKNOWN");
        member(emission, 1, "EUNKNOWN");
        resp("pbsr_resp", emission, "EUNKNOWN", "ZZZZ", 300);
        jdbc.update("UPDATE prw_emission SET visible_at = now() - INTERVAL '21 hours' WHERE id = ?", emission);

        ExtRow row = ext(arrival, "EUNKNOWN");
        assertThat(row.status()).isEqualTo("ZZZZ");
        assertThat(row.terminal()).isFalse();
        assertThat(due(client)).isEmpty();
        assertThat(slaCount(client)).isEqualTo(1L);
        assertThat(jdbc.queryForObject(
                "SELECT age_hours FROM prg_sla_pending WHERE client=? AND e2e='EUNKNOWN'",
                Double.class, client)).isGreaterThan(20.0);
        assertThat(jdbc.queryForObject(
                "SELECT classification FROM prg_status_exception WHERE client=? AND e2e='EUNKNOWN'",
                String.class, client)).isEqualTo("UNKNOWN");
        assertThat(watermarks.findDeltaSlice(client, "", 10)).isEmpty();
        assertThat(watermarks.findRangeSlice(client, "", 10)).isEmpty();
        assertThat(watermarks.findUnreportedForParent(client, "MSGUNKNOWN", 10)).isEmpty();
        assertThat(watermarks.findCurrentForParent(client, "MSGUNKNOWN", "", 10)).isEmpty();
    }

    // --- (e) SLA pending view ages non-terminal members of VISIBLE batches ---

    @Test
    void slaPendingAgesOnlyNonTerminalMembers() {
        String client = "FNBRF03";
        UUID as = parent(client, "MSGS");
        tx(as, 1, "ES1");
        tx(as, 2, "ES2");
        UUID bs = batch(group(as, client, "MSGS", 1), as, 1, "MSGS");
        member(bs, 1, "ES1");
        member(bs, 2, "ES2");
        jdbc.update("UPDATE prw_emission SET visible_at = now() - INTERVAL '21 hours' WHERE id = ?", bs);
        resp("pbsr_resp", bs, "ES1", "ACSC", null); // terminal: never SLA-pending

        var rows = jdbc.query("SELECT e2e, outbound_msg_id, age_hours FROM prg_sla_pending WHERE client = ?",
                (rs, i) -> Map.entry(rs.getString("e2e"), rs.getDouble("age_hours")), client);
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getKey()).isEqualTo("ES2");
        assertThat(rows.get(0).getValue()).isBetween(20.9, 22.0);
    }
}
