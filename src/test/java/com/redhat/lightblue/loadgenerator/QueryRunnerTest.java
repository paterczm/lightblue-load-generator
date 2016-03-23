package com.redhat.lightblue.loadgenerator;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.LightblueException;
import com.redhat.lightblue.client.request.data.DataFindRequest;
import com.redhat.lightblue.loadgenerator.Query.Range;

import junit.framework.Assert;

@RunWith(MockitoJUnitRunner.class)
public class QueryRunnerTest {

    @Mock
    LightblueClient client;

    List<Query> queries;

    @Before
    public void loadQueries() throws IOException {
        final Properties properties = new Properties();
        try (final InputStream stream = this.getClass().getClassLoader().getResourceAsStream("./queries.properties")) {
            properties.load(stream);
        }

        queries = Query.fromProperties(properties);
    }

    @Test
    public void testQueryRunner() throws LightblueException {
        Query q1 = new Query("q1", "user", null, new Range(0, 7000000, 1000), 0, 10, 0);

        new QueryRunner(q1, client).run();

        ArgumentCaptor<DataFindRequest> argument = ArgumentCaptor.forClass(DataFindRequest.class);

        Mockito.verify(client, Mockito.times(10)).data(argument.capture());

        for (DataFindRequest r : argument.getAllValues()) {
            Assert.assertEquals("user", r.getEntityName());
            Assert.assertNull(r.getEntityVersion());

            int range[] = getRange(r);
            Assert.assertEquals(range[1] - range[0] + 1, 1000);
            Assert.assertTrue(range[0] >= 0);
            Assert.assertTrue(range[1] < 7000000);
        }
    }

    @Test
    public void testQueryRunner2() throws LightblueException {
        Query q2 = new Query("q2", "legalEntity", "0.0.3", new Range(1000000, 5000000, 2000), 0, 20, 0);

        new QueryRunner(q2, client).run();

        ArgumentCaptor<DataFindRequest> argument = ArgumentCaptor.forClass(DataFindRequest.class);

        Mockito.verify(client, Mockito.times(20)).data(argument.capture());

        for (DataFindRequest r : argument.getAllValues()) {
            Assert.assertEquals("legalEntity", r.getEntityName());
            Assert.assertEquals("0.0.3", r.getEntityVersion());

            int range[] = getRange(r);
            Assert.assertEquals(range[1] - range[0] + 1, 2000);
            Assert.assertTrue(range[0] >= 1000000);
            Assert.assertTrue(range[1] < 5000000);
        }
    }

    private int[] getRange(DataFindRequest dfr) {
        ArrayNode range = (ArrayNode) dfr.getBodyJson().get("range");

        return new int[] { range.get(0).asInt(), range.get(1).asInt() };
    }

}
