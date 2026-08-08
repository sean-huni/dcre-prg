package za.co.fnb.dcre.prg.data.model;

/** Read projection of one prg_delivery_ledger row, e.g. for exact manual replay (SCRUM-55). */
public record LedgerRow(String e2e, String status) {
}
