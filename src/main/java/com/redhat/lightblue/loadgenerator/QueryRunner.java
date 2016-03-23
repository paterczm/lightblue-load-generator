package com.redhat.lightblue.loadgenerator;

import java.util.Date;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.LightblueException;
import com.redhat.lightblue.client.Projection;
import com.redhat.lightblue.client.request.data.DataFindRequest;
import com.redhat.lightblue.loadgenerator.Query.Range;

public class QueryRunner implements Runnable {

    public static final int MIN_DELAY_ON_ERROR_MS = 5000;
    
    public static final int INITIAL_DELAY_PER_THREAD_MS = 5000;

    public static final Logger log = LoggerFactory.getLogger(QueryRunner.class);

    private final Query query;
    private final LightblueClient client;

    private static final Random random = new Random(new Date().getTime());

    public QueryRunner(Query query, LightblueClient client) {
        super();
        this.query = query;
        this.client = client;
    }

    @Override
    public void run() {

        try {
            int i = 0;
            while (true) {
                try {
                    
                    if (i == 0 && query.getThreads() >= 1) {
                        // add a random wait on first iteration to better distribute the load
                        Thread.sleep(random.nextInt(INITIAL_DELAY_PER_THREAD_MS*query.getThreads()));
                    }
                    
                    Range r = query.getRange();

                    int minRange = r.max - r.min - r.size;

                    int from = random.nextInt(minRange) + r.min;
                    int to = from + r.size - 1;

                    DataFindRequest dfr = query.getVersion() != null ? new DataFindRequest(query.getEntity(), query.getVersion())
                            : new DataFindRequest(query.getEntity());

                    dfr.select(Projection.includeFieldRecursively("*"));
                    dfr.range(from, to);

                    log.info(String.format("Iteration %d: Running query %s from %d to %d", i, query, from, to));
                    client.data(dfr);

                    Thread.sleep(query.getDelayMS());

                } catch (LightblueException e) {
                    log.error("Error calling lightblue, waiting " + MIN_DELAY_ON_ERROR_MS + "ms", e);
                    Thread.sleep(MIN_DELAY_ON_ERROR_MS+random.nextInt(10000));
                }

                if (query.getLoop() > 0 && ++i >= query.getLoop()) {
                    break;
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

}
