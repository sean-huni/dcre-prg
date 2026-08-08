package za.co.fnb.dcre.prg.data.model;

import java.util.UUID;

/** Read projection of a mid-DAG ext_tx_status row with no reportable status (R-38). */
public record UnknownRow(UUID arrivalId, Integer sequence, String e2e) {
}
