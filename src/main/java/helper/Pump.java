package helper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public abstract class Pump {

    protected final Connection conn;
    protected final PreparedStatement statement;
    protected final int offset;

    protected Pump(Connection conn, PreparedStatement statement, int offset) {
        this.conn = conn;
        this.statement = statement;
        this.offset = offset;
    }

    public abstract int run() throws SQLException;

    protected int commitBatch() throws SQLException {
        int count = statement.executeBatch().length;
        conn.commit();
        return count;
    }

}
