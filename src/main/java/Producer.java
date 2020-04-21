import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static java.util.concurrent.TimeUnit.SECONDS;

public class Producer {

    public static void main(String[] args) throws Exception {
        String url = "jdbc:mysql://localhost:3306/inventory";
        String user = "mysqluser";
        String password = "mysqlpw";

        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            conn.setAutoCommit(false);

            /*new InsertBatch(conn, 2000, 100).run();
            conn.commit();*/

            Monitor monitor = new Monitor(1, SECONDS);
            UpdateBatch updateBatch = new UpdateBatch(conn, 2000, 100);
            for (; ; ) {
                int rowsUpdated = updateBatch.run();
                conn.commit();
                monitor.inc(rowsUpdated);
            }
        }
    }

    private static PreparedStatement toStatement(Connection conn, String sql) {
        try {
            return conn.prepareStatement(sql);
        } catch (SQLException ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }

    private static void addBatch(Statement statement, String sql) {
        try {
            statement.addBatch(sql);
        } catch (SQLException ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }

    private interface Batch {

        int run() throws SQLException;

    }

    private static final class InsertBatch implements Batch {

        private static final String SQL = "INSERT INTO customers (id, first_name, last_name, email) " +
                "VALUES (%d, '%s', '%s', '%s')";

        private final Statement statement;

        public InsertBatch(Connection conn, int offset, int size) throws SQLException {
            statement = conn.createStatement();
            IntStream.range(offset, offset + size)
                    .forEach(id -> addBatch(statement, String.format(SQL, id, "fn" + id, "ln" + id, "@" + id)));
        }

        @Override
        public int run() throws SQLException {
            return statement.executeBatch().length;
        }
    }

    private static class UpdateBatch implements Batch {

        private static final String SQL = "UPDATE customers SET first_name=?, last_name=?, email='%s@haven.com' WHERE id=%s";

        private final SimpleDateFormat df = new SimpleDateFormat("HHmmss");
        private final PreparedStatement[] statements;

        public UpdateBatch(Connection conn, int offset, int size) {
            statements = IntStream.range(offset, offset + size)
                    .limit(size)
                    .mapToObj(id -> String.format(SQL, id, id))
                    .map(sql -> toStatement(conn, sql))
                    .toArray(PreparedStatement[]::new);
        }

        @Override
        public int run() throws SQLException {
            String randomThing = ThreadLocalRandom.current().nextInt() +
                    "_" +
                    new Date(System.currentTimeMillis()).toString();
            int rowsUpdated = 0;
            for (PreparedStatement statement : statements) {
                statement.setString(1, "fn_" + randomThing);
                statement.setString(2, "ln_" + randomThing);
                rowsUpdated += statement.executeUpdate();
            }
            return rowsUpdated;
        }
    }

    private static class Monitor {

        private final String reportPattern;
        private final long reportingPeriodMillis;

        private long nextReportTime;
        private int totalInPeriod;

        public Monitor(long duration, TimeUnit unit) {
            this.reportingPeriodMillis = TimeUnit.MILLISECONDS.convert(duration, unit);
            this.reportPattern = String.format("Updates in last %d %s: %s", duration, unit.name(), "%,d");
            this.nextReportTime = System.currentTimeMillis() + reportingPeriodMillis;
        }

        void inc(int count) {
            totalInPeriod += count;
            if (System.currentTimeMillis() > nextReportTime) {
                System.out.println(String.format(reportPattern, totalInPeriod));

                totalInPeriod = 0;
                nextReportTime = System.currentTimeMillis() + reportingPeriodMillis;
            }
        }

    }

}
