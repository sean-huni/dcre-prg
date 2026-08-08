package za.co.fnb.dcre.prg.data.repo;

import org.springframework.jdbc.core.RowMapper;
import za.co.fnb.dcre.prg.data.model.StatusRow;

import java.sql.ResultSet;
import java.sql.SQLException;

public class StatusRowMapper implements RowMapper<StatusRow> {

    @Override
    public StatusRow mapRow(ResultSet r, int rowNum) throws SQLException {
        return new StatusRow(r.getString("e2e"), r.getString("status"));
    }
}
