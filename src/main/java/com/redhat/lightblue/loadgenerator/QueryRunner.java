package com.redhat.lightblue.loadgenerator;

import java.util.Date;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.LightblueException;
import com.redhat.lightblue.client.Operation;
import com.redhat.lightblue.client.Projection;
import com.redhat.lightblue.client.Query;
import com.redhat.lightblue.client.http.HttpMethod;
import com.redhat.lightblue.client.request.data.DataFindRequest;
import com.redhat.lightblue.client.request.data.LiteralDataRequest;
import com.redhat.lightblue.client.response.LightblueDataResponse;
import com.redhat.lightblue.loadgenerator.RQuery.Range;

public class QueryRunner implements Runnable {
    
    public static final int INITIAL_DELAY_PER_THREAD_MS = 5000, MAX_INITIAL_DELAY_MS=120000;

    public static final Logger log = LoggerFactory.getLogger(QueryRunner.class);

    private final RQuery query;
    private final LightblueClient client;
    private final boolean runForever, stats;
    private Date runUntil;

    private static final Random random = new Random(new Date().getTime());

    public QueryRunner(RQuery query, LightblueClient client, boolean runForever, Date runUntil, boolean stats) {
        super();
        this.query = query;
        this.client = client;
        this.runForever = runForever;
        this.stats = stats;
        this.runUntil = runUntil;
    }

    @Override
    public void run() {

        try {
            int i = 0;
            while (true) {
                boolean findQueryCompleted = false;
                try {
                    
                    if (i == 0 && query.getThreads() > 4) {
                        // add a random wait on first iteration to better distribute the load
                        Thread.sleep(random.nextInt(Math.max(INITIAL_DELAY_PER_THREAD_MS*query.getThreads(), MAX_INITIAL_DELAY_MS)));
                    }
                    
                    Range r = query.getRange();

                    int minRange = r.max - r.min - r.size;

                    int from = random.nextInt(minRange) + r.min;
                    int to = from + r.size - 1;

                    DataFindRequest dfr = query.getVersion() != null ? new DataFindRequest(query.getEntity(), query.getVersion())
                            : new DataFindRequest(query.getEntity());

                    dfr.select(Projection.includeFieldRecursively("*"));
                    if (!query.getRange().isIdRange) {
                        // far ranges can take very long to fetch, b/c the cursor needs to travel far
                        dfr.range(from, to);
                    } else {
                        // use numeric field, should be indexed
                        dfr.where(Query.and(Query.withValue(r.idField, Query.gte, from), Query.withValue(r.idField, Query.lte, to)));
                    }

                    log.debug(String.format("Iteration %d: Running query %s from %d to %d", i, query, from, to));
                    long t0 = new Date().getTime();
                    LightblueDataResponse response = client.data(dfr);
                    long t1 = new Date().getTime();
                    findQueryCompleted = true;

                    if (stats) {
                        Stats.getInstance().successfullCall("find-"+query.getName(), (int)(t1-t0));
                    }

                    if (query.isWithSave()) {
                        // save data back to lightblue
                        LiteralDataRequest save = new LiteralDataRequest(query.getEntity(), query.getVersion(), createSaveRequestBody(response.getProcessed()), HttpMethod.POST, "save", Operation.SAVE);

                        log.debug(String.format("Iteration %d: Saving back results of query %s", i, query));
                        t0 = new Date().getTime();
                        client.data(save);
                        t1 = new Date().getTime();

                        if (stats) {
                            Stats.getInstance().successfullCall("save-"+query.getName(), (int)(t1-t0));
                        }
                    }

                    Thread.sleep(query.getDelayMS());

                } catch (LightblueException e) {
                    if (stats) {
                        if (findQueryCompleted)
                            Stats.getInstance().failedCall("save-"+query.getName());
                        else {
                            Stats.getInstance().failedCall("find-"+query.getName());
                        }
                    }
                    log.error("Error calling lightblue", e);
                    Thread.sleep(query.getDelayMS());
                }

                i++;

                if (!runForever && runUntil == null && (query.getLoop() > 0 && i >= query.getLoop())) {
                    log.info("Max iterations reached, stopping thread");
                    break;
                }

                if (!runForever && (runUntil != null && new Date().after(runUntil))) {
                    if (stats) {
                        Stats.getInstance().printStats();
                    }
                    log.info("Max time reached, stopping...");
                    System.exit(0);
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    private JsonNode createSaveRequestBody(JsonNode data) {
        ObjectNode node = JsonNodeFactory.instance.objectNode();

        node.set("projection", Projection.includeFieldRecursively("*").toJson());

        node.set("data", data);

        node.set("upsert", JsonNodeFactory.instance.booleanNode(true));

        return node;
    }

}
