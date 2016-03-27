package com.redhat.lightblue.loadgenerator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Stats implements Runnable {

    public static final Logger log = LoggerFactory.getLogger(Stats.class);

    public static int CALCULATE_STATS_EVERY_MS = 10000;

    private Stats() {
        log.info("Starting stats (delay="+CALCULATE_STATS_EVERY_MS+"ms)");
    }

    private Map<String, List<Integer>> successfulCalls = new ConcurrentHashMap<>();
    private Map<String, Integer> failedCalls = new ConcurrentHashMap<>();

    public void successfullCall(String queryName, int tookMS) {

        synchronized (queryName.intern()) {
            if (!successfulCalls.containsKey(queryName)) {
                successfulCalls.put(queryName, new ArrayList<Integer>());
            }

            successfulCalls.get(queryName).add(tookMS);
            Collections.sort(successfulCalls.get(queryName));
        }
    }

    public void failedCall(String queryName) {
        if (!failedCalls.containsKey(queryName)) {
            failedCalls.put(queryName, 0);
        }

        int currentFailedCount = failedCalls.containsKey(queryName) ? failedCalls.get(queryName) : 0;

        failedCalls.put(queryName, currentFailedCount + 1);
    }

    public String getStats(String queryName) {
        synchronized (queryName.intern()) {
            Collections.sort(successfulCalls.get(queryName));

            int failedCallsCount = failedCalls.get(queryName) == null ? 0 : failedCalls.get(queryName);
            int successfullCallsCount = successfulCalls.get(queryName).size();
            int totalCallsCount = failedCallsCount + successfullCallsCount;

            Integer[] successfullCallsArray = successfulCalls.get(queryName).toArray(new Integer[] {});

            int perc50 = successfullCallsArray[successfullCallsCount / 2];
            int perc75 = successfullCallsArray[successfullCallsCount * 3 / 4];
            int perc90 = successfullCallsArray[successfullCallsCount * 9 / 10];
            int perc95 = successfullCallsArray[successfullCallsCount * 19 / 20];

            return String.format("Stats for %s: totalCalls=%d, failedCalls=%d%%, perc50=%dms, perc75=%dms, perc90=%dms, perc95=%dms", queryName,
                    totalCallsCount, (int) (100f * failedCallsCount / totalCallsCount), perc50, perc75, perc90, perc95);
        }
    }

    private final static Stats stats = new Stats();

    public static Stats getInstance() {
        return stats;
    }

    public static void init() {
        new Thread(getInstance()).start();
    }

    @Override
    public void run() {

        try {

            while (true) {
                Thread.sleep(CALCULATE_STATS_EVERY_MS);

                for (String query: successfulCalls.keySet()) {
                    log.info(Stats.getInstance().getStats(query));
                }
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

}
