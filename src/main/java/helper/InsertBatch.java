package helper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public final class InsertBatch implements Batch {

    private static final String SQL = "INSERT INTO customers (id, first_name, last_name, email) " +
            "VALUES (?, ?, ?, ?)";

    private final PreparedStatement statement;
    private final int offset;
    private final int size;

    public InsertBatch(Connection conn, int offset, int size) throws SQLException {
        this.statement = conn.prepareStatement(SQL);
        this.offset = offset;
        this.size = size;
    }

    @Override
    public int run() throws SQLException {
        for (int i = offset; i < offset  + size; i++) {
            statement.setInt(1, i);
            statement.setString(2, "fn_" + i);
            statement.setString(3, "ln_" + i);
            statement.setString(4, i + "@haven.com");
            statement.addBatch();
        }
        return statement.executeBatch().length;
    }
}
