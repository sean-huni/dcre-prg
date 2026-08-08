package za.co.fnb.dcre.prg.data.repo;

import org.springframework.jdbc.core.RowMapper;
import za.co.fnb.dcre.prg.data.model.LedgerRow;

import java.sql.ResultSet;
import java.sql.SQLException;

public class LedgerRowMapper implements RowMapper<LedgerRow> {

    @Override
    public LedgerRow mapRow(ResultSet r, int rowNum) throws SQLException {
        return new LedgerRow(r.getString("e2e"), r.getString("status"));
    }
}
