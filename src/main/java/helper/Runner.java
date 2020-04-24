package helper;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static java.util.concurrent.TimeUnit.SECONDS;

public abstract class Runner {

    protected static final int ID_OFFSET = 2000;
    protected static final int BATCH_SIZE = 10_000;

    protected Monitor monitor = new Monitor(5, SECONDS);

    public void run() throws SQLException {
        String url = "jdbc:mysql://192.168.0.32:3306/inventory?rewriteBatchedStatements=true";
        String user = "mysqluser";
        String password = "mysqlpw";

        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            run(conn);
        }
    }

    public abstract void run(Connection conn) throws SQLException;

    protected void deleteAll(Connection conn) throws SQLException {
        DeletePump deletePump = new DeletePump(conn, 1005, BATCH_SIZE);
        for (;;) {
            int rows = deletePump.run();
            if (rows <= 0) {
                break;
            }
            monitor.inc(rows);
        }
    }
}
