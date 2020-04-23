import helper.DeletePump;
import helper.InsertPump;
import helper.Monitor;
import helper.Runner;
import helper.UpdatePump;

import java.sql.Connection;
import java.sql.SQLException;

import static java.util.concurrent.TimeUnit.SECONDS;

public class MixedBounded extends Runner {

    public static void main(String[] args) throws Exception {
        new MixedBounded().run();
    }

    @Override
    public void run(Connection conn) throws SQLException {
        conn.setAutoCommit(false);

        deleteAll(conn);
        monitor.inc(new InsertPump(conn, ID_OFFSET, BATCH_SIZE).run());

        UpdatePump updatePump = new UpdatePump(conn, ID_OFFSET, BATCH_SIZE);
        InsertPump insertBatch = new InsertPump(conn, ID_OFFSET + BATCH_SIZE, BATCH_SIZE);
        DeletePump deletePump = new DeletePump(conn, ID_OFFSET + BATCH_SIZE, BATCH_SIZE);
        for (; ; ) {
            monitor.inc(updatePump.run());
            monitor.inc(insertBatch.run());
            monitor.inc(deletePump.run());
        }
    }

}
