package za.co.fnb.dcre.prg.data.repo;

import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import za.co.fnb.dcre.prg.data.model.PrgWatermarkEntity;
import za.co.fnb.dcre.prg.data.model.StatusRow;
import za.co.fnb.dcre.prg.data.model.UnknownRow;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface PrgWatermarkRepo extends CrudRepository<PrgWatermarkEntity, UUID> {

    /**
     * Bounded keyset slice of the delta: rows whose external status moved past
     * the client's watermark (SCRUM-42 load fix: the whole-book variant blew
     * CRDB's sql memory budget on the 30M-tx book; resume after the caller's
     * last e2e, '' for the first slice). status IS NOT NULL: a window can fire
     * mid-DAG before CTV verdicts exist; an unknown status is not reportable
     * (and would violate the watermark NOT NULL).
     */
    @Query(value = """
            SELECT x.e2e, x.status
            FROM ext_tx_status x
            LEFT JOIN prg_watermark w ON w.client = x.client AND w.e2e = x.e2e
            WHERE x.client = :client AND x.status IS NOT NULL
              AND (x.stage_rank = 1 OR EXISTS (SELECT 1 FROM prg_status_class sc
                                               WHERE sc.code = x.status AND sc.reportable))
              AND (w.e2e IS NULL OR w.last_status <> x.status)
              AND x.e2e > :afterE2e
            ORDER BY x.e2e
            LIMIT :limit""", rowMapperClass = StatusRowMapper.class)
    List<StatusRow> findDeltaSlice(@Param("client") String client, @Param("afterE2e") String afterE2e,
                                   @Param("limit") int limit);

    /** Resend override, same keyset slicing: all current reportable statuses, watermark ignored. */
    @Query(value = """
            SELECT x.e2e, x.status
            FROM ext_tx_status x
            WHERE x.client = :client AND x.status IS NOT NULL
              AND (x.stage_rank = 1 OR EXISTS (SELECT 1 FROM prg_status_class sc
                                               WHERE sc.code = x.status AND sc.reportable))
              AND x.e2e > :afterE2e
            ORDER BY x.e2e
            LIMIT :limit""", rowMapperClass = StatusRowMapper.class)
    List<StatusRow> findRangeSlice(@Param("client") String client, @Param("afterE2e") String afterE2e,
                                   @Param("limit") int limit);

    /**
     * Aggregate count of mid-DAG rows with no reportable status (R-38 at
     * scale): the row-returning whole-book scan died live with
     * "sql: memory budget exceeded" on the 30M-tx book.
     */
    @Query("SELECT count(*) FROM ext_tx_status x WHERE x.client = :client AND x.status IS NULL")
    long countUnknown(@Param("client") String client);

    /** Bounded per-row detail for small unknown counts: keeps the uniform R-38 WARN shape below the limit. */
    @Query(value = """
            SELECT x.arrival_id, x.sequence, x.e2e
            FROM ext_tx_status x
            WHERE x.client = :client AND x.status IS NULL
            ORDER BY x.arrival_id, x.sequence
            LIMIT :limit""", rowMapperClass = UnknownRowMapper.class)
    List<UnknownRow> findUnknownDetail(@Param("client") String client, @Param("limit") int limit);

    /**
     * SCRUM-55 parent-scoped delta for IMMEDIATE reports: every current reportable
     * status of the parent's rows not yet auto-ledgered. Deliberately ledger-guarded
     * (not watermark-guarded): the delivery ledger is the authority on what
     * was externally reported, and re-reading after each ledgered slice makes
     * reported rows drop out, which is also the pagination (no keyset needed).
     */
    @Query(value = """
            SELECT x.e2e, x.status FROM ext_tx_status x
            WHERE x.client = :client AND x.source_msg_id = :sourceMsgId AND x.status IS NOT NULL
              AND (x.stage_rank = 1 OR EXISTS (SELECT 1 FROM prg_status_class sc
                                               WHERE sc.code = x.status AND sc.reportable))
              AND NOT EXISTS (SELECT 1 FROM prg_delivery_ledger l
                              WHERE l.client = x.client AND l.e2e = x.e2e AND l.status = x.status
                                AND l.manual_ref IS NULL)
            ORDER BY x.e2e LIMIT :limit""", rowMapperClass = StatusRowMapper.class)
    List<StatusRow> findUnreportedForParent(@Param("client") String client,
            @Param("sourceMsgId") String sourceMsgId, @Param("limit") int limit);

    /**
     * SCRUM-55 MANUAL regenerate-from-current-status: all current reportable
     * statuses of the parent, ledger ignored (the manual override bypasses the
     * auto guard). Keyset pagination: manual ledger rows never drop out of reads.
     */
    @Query(value = """
            SELECT x.e2e, x.status FROM ext_tx_status x
            WHERE x.client = :client AND x.source_msg_id = :sourceMsgId AND x.status IS NOT NULL
              AND (x.stage_rank = 1 OR EXISTS (SELECT 1 FROM prg_status_class sc
                                               WHERE sc.code = x.status AND sc.reportable))
              AND x.e2e > :afterE2e
            ORDER BY x.e2e LIMIT :limit""", rowMapperClass = StatusRowMapper.class)
    List<StatusRow> findCurrentForParent(@Param("client") String client,
            @Param("sourceMsgId") String sourceMsgId, @Param("afterE2e") String afterE2e,
            @Param("limit") int limit);

    /**
     * SCRUM-55 heartbeat PD line: the client's still-pending rows from
     * prg_sla_pending (members of VISIBLE batches whose current status is
     * non-terminal). Zero pending prints PD|0.
     */
    @Query("SELECT count(*) FROM prg_sla_pending WHERE client = :client")
    long countSlaPending(@Param("client") String client);

    /** COMPLETE or IDLE from the due view: the trigger_kind stamped on an immediate prg_report row. */
    @Query("SELECT reason FROM prg_report_due WHERE client = :client AND source_msg_id = :sourceMsgId LIMIT 1")
    Optional<String> dueReason(@Param("client") String client, @Param("sourceMsgId") String sourceMsgId);

    /** NEVER UPSERT INTO: CRDB resolves UPSERT on PK only; business identity is (client, e2e). */
    @Modifying
    @Query("""
            INSERT INTO prg_watermark (id, client, e2e, last_status)
            VALUES (gen_random_uuid(), :client, :e2e, :status)
            ON CONFLICT (client, e2e)
            DO UPDATE SET last_status = excluded.last_status, updated_at = now()""")
    void upsertWatermark(@Param("client") String client, @Param("e2e") String e2e,
                         @Param("status") String status);
}
