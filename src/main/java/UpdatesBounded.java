import helper.InsertPump;
import helper.Runner;
import helper.UpdatePump;

import java.sql.Connection;
import java.sql.SQLException;

public class UpdatesBounded extends Runner {

    public static void main(String[] args) throws SQLException {
        new UpdatesBounded().run();
    }

    @Override
    public void run(Connection conn) throws SQLException {
        conn.setAutoCommit(false);

        deleteAll(conn);
        monitor.inc(new InsertPump(conn, ID_OFFSET, BATCH_SIZE).run());

        UpdatePump updatePump = new UpdatePump(conn, ID_OFFSET, BATCH_SIZE);
        for (; ; ) {
            monitor.inc(updatePump.run());
        }
    }
}
