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

    public static final int MIN_DELAY_ON_ERROR_MS = 5000;
    
    public static final int INITIAL_DELAY_PER_THREAD_MS = 5000, MAX_INITIAL_DELAY_MS=120000;

    public static final Logger log = LoggerFactory.getLogger(QueryRunner.class);

    private final RQuery query;
    private final LightblueClient client;
    private final boolean runForever;

    private static final Random random = new Random(new Date().getTime());

    public QueryRunner(RQuery query, LightblueClient client, boolean runForever) {
        super();
        this.query = query;
        this.client = client;
        this.runForever = runForever;
    }

    @Override
    public void run() {

        try {
            int i = 0;
            while (true) {
                try {
                    
                    if (i == 0 && query.getThreads() >= 1) {
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
                    if (!query.isWithSave()) {
                        // far ranges can take very long to fetch, b/c the cursor needs to travel far
                        dfr.range(from, to);
                    } else {
                        // use index
                        // works only for numeric _ids
                        dfr.where(Query.and(Query.withValue("_id", Query.gte, from), Query.withValue("_id", Query.lte, to)));
                    }

                    log.info(String.format("Iteration %d: Running query %s from %d to %d", i, query, from, to));
                    LightblueDataResponse response = client.data(dfr);

                    if (query.isWithSave()) {
                        // save data back to lightblue
                        LiteralDataRequest save = new LiteralDataRequest(query.getEntity(), query.getVersion(), createSaveRequestBody(response.getProcessed()), HttpMethod.POST, "save", Operation.SAVE);

                        log.info(String.format("Iteration %d: Saving back results of query %s", i, query));
                        client.data(save);
                    }

                    Thread.sleep(query.getDelayMS());

                } catch (LightblueException e) {
                    log.error("Error calling lightblue, waiting " + MIN_DELAY_ON_ERROR_MS + "ms", e);
                    Thread.sleep(MIN_DELAY_ON_ERROR_MS+random.nextInt(10000));
                }

                i++;

                if (!runForever && query.getLoop() > 0 && i >= query.getLoop()) {
                    break;
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
