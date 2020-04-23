import helper.DeletePump;
import helper.InsertPump;
import helper.Runner;

import java.sql.Connection;
import java.sql.SQLException;

public class InsertsBounded extends Runner {

    public static void main(String[] args) throws Exception {
        new InsertsBounded().run();
    }

    @Override
    public void run(Connection conn) throws SQLException {
        conn.setAutoCommit(false);

        deleteAll(conn);

        InsertPump insertBatch = new InsertPump(conn, ID_OFFSET, BATCH_SIZE);
        DeletePump deletePump = new DeletePump(conn, ID_OFFSET, BATCH_SIZE);
        for (; ; ) {
            monitor.inc(insertBatch.run());
            monitor.inc(deletePump.run());
        }
    }

}
