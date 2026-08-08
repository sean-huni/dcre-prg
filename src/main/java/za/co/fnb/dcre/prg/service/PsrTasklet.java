package za.co.fnb.dcre.prg.service;

import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.stereotype.Component;
import za.co.fnb.dcre.platform.batch.OutcomeFileWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Thin entry adapter (3-tier). Job identity: (client, window); everything
 * else is a non-identifying override. Dispatch on report.type (SCRUM-55):
 * SCHEDULED (default) = the clock-window delta path (resend "true" re-emits
 * all current rows); IMMEDIATE = one ledger-guarded parent report per entry
 * in the comma-joined "parents" list; MANUAL = replay when "report.id" is
 * set, else regenerate-from-current-status with "manual.ref". Multiple
 * parents get per-parent window keys (window-1..N): prg_report.file_name is
 * unique per report artifact.
 */
@Component
public class PsrTasklet implements Tasklet {

    private final PsrReportService service;
    private final ImmediateReportService immediate;

    public PsrTasklet(final PsrReportService service, final ImmediateReportService immediate) {
        this.service = service;
        this.immediate = immediate;
    }

    @Override
    public RepeatStatus execute(final StepContribution contribution, final ChunkContext chunkContext)
            throws Exception {
        final Map<String, Object> params = chunkContext.getStepContext().getJobParameters();
        // SCRUM-58: the seam job name (env JOB_NAME or local-prg-<executionId>) is
        // resolved once here in the adapter and threaded into every report row so
        // it commits in the same transaction as the prg_report insert.
        final long executionId = chunkContext.getStepContext().getStepExecution()
                .getJobExecution().getId();
        final String jobName = OutcomeFileWriter.jobNameOrLocal("prg", executionId);
        final String type = (String) params.getOrDefault("report.type", "SCHEDULED");
        final String emitted = switch (type) {
            case "SCHEDULED" -> scheduled(params, jobName);
            case "IMMEDIATE" -> reportParents(params, null, jobName);
            case "MANUAL" -> manual(params, jobName);
            default -> throw new IllegalArgumentException("unsupported report.type=%s".formatted(type));
        };
        chunkContext.getStepContext().getStepExecution().getJobExecution()
                .getExecutionContext().putString("psr.file", emitted);
        return RepeatStatus.FINISHED;
    }

    private String scheduled(final Map<String, Object> params, final String jobName) throws IOException {
        final String client = (String) params.get("client");
        final String window = (String) params.get("window");
        final boolean resend = "true".equals(params.get("resend"));
        return service.window(client, window, resend, jobName).map(Object::toString).orElse("NONE");
    }

    private String manual(final Map<String, Object> params, final String jobName) throws IOException {
        final String reportId = (String) params.get("report.id");
        if (reportId != null) {
            return immediate.replay(UUID.fromString(reportId), jobName).toString();
        }
        return reportParents(params, (String) params.get("manual.ref"), jobName);
    }

    private String reportParents(final Map<String, Object> params, final String manualRef,
                                 final String jobName) throws IOException {
        final String client = (String) params.get("client");
        final String window = (String) params.get("window");
        final String joined = (String) params.get("parents");
        if (joined == null || joined.isBlank()) {
            throw new IllegalArgumentException("parents is required for report.type=IMMEDIATE/MANUAL");
        }
        final String[] parents = joined.split(",");
        final List<String> emitted = new ArrayList<>();
        for (int i = 0; i < parents.length; i++) {
            final String windowKey = parents.length == 1 ? window : window + "-" + (i + 1);
            immediate.reportParent(client, parents[i].trim(), windowKey, manualRef, jobName)
                    .ifPresent(path -> emitted.add(path.toString()));
        }
        return emitted.isEmpty() ? "NONE" : String.join(",", emitted);
    }
}
