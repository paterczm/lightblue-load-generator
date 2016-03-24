package com.redhat.lightblue.loadgenerator;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;

public class QueryTest {
        
    @Test
    public void testLoadQueries() throws IOException {
        
        final Properties properties = new Properties();
        try (final InputStream stream =
                   this.getClass().getClassLoader().getResourceAsStream("./queries.properties")) {
            properties.load(stream);
        }
        
        List<RQuery> queries = RQuery.fromProperties(properties);
        
        Assert.assertEquals(3, queries.size());
    }
    
}
