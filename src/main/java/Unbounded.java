import helper.InsertPump;
import helper.Runner;

import java.sql.Connection;
import java.sql.SQLException;

public class Unbounded extends Runner {

    public static final int TABLE_LENGTH = 10_000_000;

    public static void main(String[] args) throws SQLException {
        new Unbounded().run();
    }

    @Override
    public void run(Connection conn) throws SQLException {
        conn.setAutoCommit(false);

        deleteAll(conn);

        InsertPump insertBatch = new InsertPump(conn, ID_OFFSET, TABLE_LENGTH, BATCH_SIZE);
        for (; ; ) {
            int rows = insertBatch.run();
            if (rows <= 0) {
                break;
            }
            monitor.inc(rows);
        }
    }
}
