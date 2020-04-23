package helper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DeleteBatch implements Batch {

    private static final String SQL = "DELETE FROM customers WHERE id >= %d;";

    private final PreparedStatement statement;

    public DeleteBatch(Connection conn, int offset) throws SQLException {
        this.statement = conn.prepareStatement(String.format(SQL, offset));
    }

    @Override
    public int run() throws SQLException {
        return statement.executeUpdate();
    }
}
