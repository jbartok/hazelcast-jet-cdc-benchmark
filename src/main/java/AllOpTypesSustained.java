import helper.DeleteBatch;
import helper.InsertBatch;
import helper.Runner;
import helper.UpdateBatch;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;

public class AllOpTypesSustained extends Runner {

    public static final int ID_OFFSET = 2000;
    public static final int BATCH_SIZE = 5000;

    @Override
    public void run(Connection conn) throws SQLException {
        conn.setAutoCommit(false);

        //delete all data
        new DeleteBatch(conn, 1005).run();
        //insert rows for update batch
        new InsertBatch(conn, ID_OFFSET, BATCH_SIZE).run();
        conn.commit();

        Monitor monitor = new Monitor(3, SECONDS);
        UpdateBatch updateBatch = new UpdateBatch(conn, ID_OFFSET, BATCH_SIZE);
        InsertBatch insertBatch = new InsertBatch(conn, ID_OFFSET + BATCH_SIZE, BATCH_SIZE);
        DeleteBatch deleteBatch = new DeleteBatch(conn, ID_OFFSET + BATCH_SIZE);
        for (; ; ) {
            //update first batch
            monitor.inc(updateBatch.run());
            conn.commit();

            //insert second batch
            monitor.inc(insertBatch.run());
            conn.commit();

            //delete second batch
            monitor.inc(deleteBatch.run());
            conn.commit();
        }
    }

    public static void main(String[] args) throws Exception {
        new AllOpTypesSustained().run();
    }

    private static class Monitor {

        private final long reportingPeriodMillis;

        private long nextReportTime;
        private int totalInPeriod;

        public Monitor(long duration, TimeUnit unit) {
            this.reportingPeriodMillis = TimeUnit.MILLISECONDS.convert(duration, unit);
            this.nextReportTime = System.currentTimeMillis() + reportingPeriodMillis;
        }

        void inc(int count) {
            totalInPeriod += count;
            if (System.currentTimeMillis() > nextReportTime) {
                long perSecond = (1000L * totalInPeriod) / reportingPeriodMillis;
                System.out.println(String.format("%,d updates/sec", perSecond));

                totalInPeriod = 0;
                nextReportTime = System.currentTimeMillis() + reportingPeriodMillis;
            }
        }

    }

}
