package za.co.fnb.dcre.prg.data.repo;

import org.springframework.jdbc.core.RowMapper;
import za.co.fnb.dcre.prg.data.model.UnknownRow;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

public class UnknownRowMapper implements RowMapper<UnknownRow> {

    @Override
    public UnknownRow mapRow(ResultSet r, int rowNum) throws SQLException {
        return new UnknownRow(r.getObject("arrival_id", UUID.class), r.getInt("sequence"),
                r.getString("e2e"));
    }
}
