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
import com.redhat.lightblue.loadgenerator.RQuery.Range;

import junit.framework.Assert;

@RunWith(MockitoJUnitRunner.class)
public class QueryRunnerTest {

    @Mock
    LightblueClient client;

    List<RQuery> queries;

    @Before
    public void loadQueries() throws IOException {
        final Properties properties = new Properties();
        try (final InputStream stream = this.getClass().getClassLoader().getResourceAsStream("./queries.properties")) {
            properties.load(stream);
        }

        queries = RQuery.fromProperties(properties);
    }

    @Test
    public void testQueryRunner() throws LightblueException {
        RQuery q1 = new RQuery("q1", "user", null, new Range(0, 7000000, 1000, false), 0, 10, 0, false);

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
        RQuery q2 = new RQuery("q2", "legalEntity", "0.0.3", new Range(1000000, 5000000, 2000, false), 0, 20, 0, false);

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
