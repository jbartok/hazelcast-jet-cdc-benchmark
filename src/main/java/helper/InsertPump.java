package helper;

import java.sql.Connection;
import java.sql.SQLException;

public final class InsertPump extends Pump {

    private static final String SQL = "INSERT INTO customers (id, first_name, last_name, email) " +
            "VALUES (?, ?, ?, ?)";

    private final int size;
    private final int batchSize;

    private int done;

    public InsertPump(Connection conn, int offset, int size) throws SQLException {
        this(conn, offset, size, size);
    }

    public InsertPump(Connection conn, int offset, int size, int batchSize) throws SQLException {
        super(conn, conn.prepareStatement(SQL), offset);
        this.size = size;
        this.batchSize = batchSize;
    }

    @Override
    public int run() throws SQLException {
        if (done >= size) {
            done = 0;
            return -1;
        }

        int todo = Math.min(batchSize, size - done);

        for (int i = offset + done; i < offset + done + todo; i++) {
            statement.setInt(1, i);
            statement.setString(2, "fn_" + i);
            statement.setString(3, "ln_" + i);
            statement.setString(4, i + "@haven.com");
            statement.addBatch();
        }
        done += todo;
        return commitBatch();
    }
}
