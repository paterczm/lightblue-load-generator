package com.redhat.lightblue.loadgenerator;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.nohope.typetools.SortedList;
import org.nohope.typetools.SortedList.SerializableComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Stats {

    public static final Logger log = LoggerFactory.getLogger(Stats.class);

    public static final int CALCULATE_STATS_EVERY_N_ITERATIONS = 20;

    private Stats() {
    }

    private Map<String, List<Integer>> successfulCalls = new ConcurrentHashMap<>();
    private Map<String, Integer> failedCalls = new ConcurrentHashMap<>();

    public void successfullCall(String queryName, int tookMS) {

        synchronized (queryName) {
            if (!successfulCalls.containsKey(queryName)) {
                successfulCalls.put(queryName, new SortedList<Integer>(new SerializableComparator<Integer>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public int compare(Integer o1, Integer o2) {
                        try {
                            return o1.compareTo(o2);
                        } catch (NullPointerException e) {
                            log.error(o1+" "+o2, e);
                            throw e;
                        }
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

    public String getStats(String queryName) {
        synchronized (queryName) {
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

}
