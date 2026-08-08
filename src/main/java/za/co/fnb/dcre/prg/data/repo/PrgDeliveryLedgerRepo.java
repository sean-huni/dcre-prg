package za.co.fnb.dcre.prg.data.repo;

import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.query.Param;
import za.co.fnb.dcre.prg.data.model.LedgerRow;

import java.util.List;
import java.util.UUID;

/**
 * SCRUM-55 append-only delivery ledger: the authority on what was externally
 * reported. Query-only repository (no aggregate mapping, plan Task 9 "no
 * entity needed"); anchored on the {@link LedgerRow} projection.
 *
 * <p>Auto sends are arbitrated by the DATABASE via the partial unique index
 * uq_prg_ledger_auto ON (client, e2e, status) WHERE manual_ref IS NULL: never
 * auto-send the same tuple twice, across restarts and racing writers alike.
 * Manual rows (manual_ref set) fall outside the partial index, so overrides
 * bypass the guard while still writing an audit row.
 */
public interface PrgDeliveryLedgerRepo extends Repository<LedgerRow, UUID> {

    /** Auto-guarded append: a colliding auto row is a silent no-op (guard semantics above). */
    @Modifying
    @Query("""
            INSERT INTO prg_delivery_ledger (id, report_id, client, e2e, status, manual_ref)
            VALUES (gen_random_uuid(), :reportId, :client, :e2e, :status, :manualRef)
            ON CONFLICT (client, e2e, status) WHERE manual_ref IS NULL DO NOTHING""")
    void record(@Param("reportId") UUID reportId, @Param("client") String client,
                @Param("e2e") String e2e, @Param("status") String status,
                @Param("manualRef") String manualRef);

    @Query("SELECT count(*) FROM prg_delivery_ledger WHERE report_id = :reportId")
    long countForReport(@Param("reportId") UUID reportId);

    /** Ledgered lines of one report in emission order: the exact-replay source. */
    @Query(value = "SELECT e2e, status FROM prg_delivery_ledger WHERE report_id = :reportId ORDER BY e2e",
            rowMapperClass = LedgerRowMapper.class)
    List<LedgerRow> rowsForReport(@Param("reportId") UUID reportId);
}
