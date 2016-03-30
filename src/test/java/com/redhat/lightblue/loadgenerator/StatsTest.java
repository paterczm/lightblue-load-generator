package com.redhat.lightblue.loadgenerator;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

import junit.framework.Assert;

@RunWith(MockitoJUnitRunner.class)
public class StatsTest {

    @Mock
    Logger log;
    @Mock
    Logger logCsv;

    String QUERY_NAME = "find-query";

    @Test
    public void test() {
        Stats stats = new Stats(0);
        stats.log = log;
        stats.logCsv = logCsv;

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

        stats.printStats(QUERY_NAME);

        Mockito.verify(log).info("find-query                                        : successfulCalls=11      , failedCalls=1       , perc50=2000 ms, perc75= 3000ms, perc90= 3000ms, perc95= 3001ms");

        ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);

        Mockito.verify(logCsv).info(argumentCaptor.capture());

        String info = argumentCaptor.getValue();
        Assert.assertEquals("find-query,11,1,2000,3000,3000,3001", info.substring(info.indexOf(",")+1));
    }

}
