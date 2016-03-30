package com.redhat.lightblue.loadgenerator;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.nohope.typetools.SortedList;
import org.nohope.typetools.SortedList.SerializableComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Stats implements Runnable {

    static Logger log = LoggerFactory.getLogger(Stats.class);
    static Logger logCsv = LoggerFactory.getLogger(Stats.class.getSimpleName()+"Csv");

    public static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

    public final int CALCULATE_STATS_EVERY_MS;

    public Stats(int calculateStatsEveryMs) {
        this.CALCULATE_STATS_EVERY_MS = calculateStatsEveryMs;
        log.info("Starting stats (delay="+CALCULATE_STATS_EVERY_MS+"ms)");
    }

    private Map<String, List<Integer>> successfulCalls = new ConcurrentHashMap<>();
    private Map<String, Integer> failedCalls = new ConcurrentHashMap<>();

    public void successfullCall(String queryName, int tookMS) {

        synchronized (queryName.intern()) {
            if (!successfulCalls.containsKey(queryName)) {
                successfulCalls.put(queryName, new SortedList<Integer>(new SerializableComparator<Integer>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public int compare(Integer o1, Integer o2) {
                        return o1.compareTo(o2);
                    }

                }));
            }

            successfulCalls.get(queryName).add(tookMS);
        }
    }

    public void failedCall(String queryName) {
        if (!failedCalls.containsKey(queryName)) {
            failedCalls.put(queryName, 0);
        }

        int currentFailedCount = failedCalls.containsKey(queryName) ? failedCalls.get(queryName) : 0;

        failedCalls.put(queryName, currentFailedCount + 1);
    }

    public void printStats(String queryName) {
        synchronized (queryName.intern()) {
            int failedCallsCount = failedCalls.get(queryName) == null ? 0 : failedCalls.get(queryName);
            int successfullCallsCount = successfulCalls.get(queryName).size();
            int totalCallsCount = failedCallsCount + successfullCallsCount;

            Integer[] successfullCallsArray = successfulCalls.get(queryName).toArray(new Integer[] {});

            int perc50 = successfullCallsArray[successfullCallsCount / 2];
            int perc75 = successfullCallsArray[successfullCallsCount * 3 / 4];
            int perc90 = successfullCallsArray[successfullCallsCount * 9 / 10];
            int perc95 = successfullCallsArray[successfullCallsCount * 19 / 20];

            log.info(String.format("%-50s: successfulCalls=%-8d, failedCalls=%-8d, perc50=%5dms, perc75=%5dms, perc90=%5dms, perc95=%5dms", queryName,
                    successfullCallsCount, failedCallsCount, perc50, perc75, perc90, perc95));
            logCsv.info(String.format("%s,%s,%d,%d,%d,%d,%d,%d", dateFormat.format(new Date()), queryName,
                    successfullCallsCount, failedCallsCount, perc50, perc75, perc90, perc95));
        }
    }

    public void printStats() {
        for (String query: successfulCalls.keySet().stream().sorted().collect(Collectors.toList())) {
            Stats.getInstance().printStats(query);
        }
    }

    private static Stats stats = null;

    public static Stats getInstance() {
        if (stats == null)
            throw new IllegalStateException("Stats not initialized. Run Stats.init() first.");
        return stats;
    }

    public static void init(int calculateStatsEveryMs) {
        stats = new Stats(calculateStatsEveryMs);
        new Thread(stats).start();
    }

    @Override
    public void run() {

        try {

            while (true) {
                Thread.sleep(CALCULATE_STATS_EVERY_MS);

                printStats();
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

}
