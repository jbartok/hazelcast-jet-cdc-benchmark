package helper;

import java.util.concurrent.TimeUnit;

public class Monitor {

    private final long reportingPeriodMillis;

    private long lastReportTime;
    private int totalInPeriod;

    public Monitor(long duration, TimeUnit unit) {
        this.reportingPeriodMillis = TimeUnit.MILLISECONDS.convert(duration, unit);
        this.lastReportTime = System.currentTimeMillis();
    }

    public void inc(int count) {
        totalInPeriod += count;
        long timeSinceLastReporting = System.currentTimeMillis() - lastReportTime;
        if (timeSinceLastReporting > reportingPeriodMillis) {
            long perSecond = (1000L * totalInPeriod) / timeSinceLastReporting;
            System.out.println(String.format("%,d events/sec", perSecond));

            totalInPeriod = 0;
            lastReportTime = System.currentTimeMillis();
        }
    }

}
