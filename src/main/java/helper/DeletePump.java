package helper;

import java.sql.Connection;
import java.sql.SQLException;

public class DeletePump extends Pump {

    private static final String SQL = "DELETE FROM customers WHERE id >= %d LIMIT %d;";

    public DeletePump(Connection conn, int offset, int batchSize) throws SQLException {
        super(conn, conn.prepareStatement(String.format(SQL, offset, batchSize)), offset);
    }

    @Override
    public int run() throws SQLException {
        int rows = statement.executeUpdate();
        conn.commit();
        return rows;
    }
}
