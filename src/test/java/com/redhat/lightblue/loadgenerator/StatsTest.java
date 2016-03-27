package com.redhat.lightblue.loadgenerator;

import org.junit.Test;

import junit.framework.Assert;

public class StatsTest {

    String QUERY_NAME = "find-query";

    @Test
    public void test() {
        Stats stats = Stats.getInstance();

        stats.failedCall(QUERY_NAME);
        stats.successfullCall(QUERY_NAME, 2000);
        stats.successfullCall(QUERY_NAME, 2000);
        stats.successfullCall(QUERY_NAME, 2000);
        stats.successfullCall(QUERY_NAME, 3000);
        stats.successfullCall(QUERY_NAME, 2000);
        stats.successfullCall(QUERY_NAME, 3000);
        stats.successfullCall(QUERY_NAME, 3000);
        stats.successfullCall(QUERY_NAME, 3001);
        stats.successfullCall(QUERY_NAME, 1500);
        stats.successfullCall(QUERY_NAME, 1500);
        stats.successfullCall(QUERY_NAME, 1500);

        String stat = stats.getStats(QUERY_NAME);

        Assert.assertEquals("Stats for find-query: totalCalls=12, failedCalls=8%, perc50=2000ms, perc75=3000ms, perc90=3000ms, perc95=3001ms", stat);
    }

}
