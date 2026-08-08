package za.co.fnb.dcre.prg.data.model;

import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;
import za.co.fnb.dcre.platform.persistence.BaseEntity;

/** Per-client delta watermark: last externally reported status per e2e. */
@Table("prg_watermark")
public class PrgWatermarkEntity extends BaseEntity {

    private String client;
    private String e2e;

    @Column("last_status")
    private String lastStatus;

    public String getClient() { return client; }
    public String getE2e() { return e2e; }
    public String getLastStatus() { return lastStatus; }
}
