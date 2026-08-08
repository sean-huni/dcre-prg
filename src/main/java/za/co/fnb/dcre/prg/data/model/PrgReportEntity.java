package za.co.fnb.dcre.prg.data.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.domain.Persistable;
import org.springframework.data.relational.core.mapping.Table;

import java.time.Instant;
import java.util.UUID;

/**
 * SCRUM-55 report registry: one row per emitted report artifact
 * (the `type` column: SCHEDULED | IMMEDIATE | HEARTBEAT | MANUAL). Plain aggregate,
 * NOT a BaseEntity: prg_report is append-only (no version/updated_at
 * columns), so new-ness rides {@link Persistable} instead of the @Version
 * heuristic and the factory assigns id + created_at client-side.
 */
@Table("prg_report")
public class PrgReportEntity implements Persistable<UUID> {

    @Id
    private UUID id;
    private String client;
    private String type;
    private String triggerKind;
    private String windowKey;
    private String parentSourceMsgId;
    private String fileName;
    private String jobName;
    private Instant createdAt;

    @Transient
    private boolean isNew;

    /** Job-less convenience (direct-service callers/tests): no batch execution, so job_name stays null. */
    public static PrgReportEntity of(final String client, final String reportType,
            final String triggerKind, final String windowKey, final String parentSourceMsgId,
            final String fileName) {
        return of(client, reportType, triggerKind, windowKey, parentSourceMsgId, fileName, null);
    }

    /**
     * SCRUM-58: {@code jobName} is the clock-scoped trace-join anchor
     * (JOB_NAME env or {@code local-prg-<executionId>}, resolved by the owning
     * tasklet); persisted in the SAME transaction as the report row so a
     * supporter can join a PSR file to its launching job's ops steps.
     */
    public static PrgReportEntity of(final String client, final String reportType,
            final String triggerKind, final String windowKey, final String parentSourceMsgId,
            final String fileName, final String jobName) {
        PrgReportEntity r = new PrgReportEntity();
        r.id = UUID.randomUUID();
        r.isNew = true;
        r.client = client;
        r.type = reportType;
        r.triggerKind = triggerKind;
        r.windowKey = windowKey;
        r.parentSourceMsgId = parentSourceMsgId;
        r.fileName = fileName;
        r.jobName = jobName;
        r.createdAt = Instant.now();
        return r;
    }

    @Override
    public UUID getId() { return id; }

    @Override
    public boolean isNew() { return isNew; }

    public String getClient() { return client; }
    public String getType() { return type; }
    public String getTriggerKind() { return triggerKind; }
    public String getWindowKey() { return windowKey; }
    public String getParentSourceMsgId() { return parentSourceMsgId; }
    public String getFileName() { return fileName; }
    public String getJobName() { return jobName; }
    public Instant getCreatedAt() { return createdAt; }
}
