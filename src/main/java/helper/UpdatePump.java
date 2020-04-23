package helper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class UpdatePump extends Pump {

    private static final String SQL = "UPDATE customers SET first_name=? WHERE id=?";

    private final int size;

    private int count;

    public UpdatePump(Connection conn, int offset, int size) throws SQLException {
        super(conn, conn.prepareStatement(SQL), offset);
        this.size = size;
        this.count = offset + size;
    }

    @Override
    public int run() throws SQLException {
        count++;
        for (int i = offset; i < offset  + size; i++) {
            statement.setString(1, "fn_" + count);
            statement.setInt(2, i);
            statement.addBatch();
        }
        return commitBatch();
    }
}
