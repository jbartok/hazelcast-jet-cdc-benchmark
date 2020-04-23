package helper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class UpdateBatch implements Batch {

    private static final String SQL = "UPDATE customers SET first_name=? WHERE id=?";

    private final PreparedStatement statement;
    private final int offset;
    private final int size;

    private int count;

    public UpdateBatch(Connection conn, int offset, int size) throws SQLException {
        this.statement = conn.prepareStatement(SQL);
        this.offset = offset;
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
        return statement.executeBatch().length;
    }
}
