package com.redhat.lightblue.loadgenerator;

import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * query1.search.simple=/find/user/0.0.3 query1.range.min=0 query1.range.max=7000000 query1.range.size=1000 query1.threads=2 query1.loop=1
 * 
 * @author mpatercz
 *
 */
public class Query {

    public static class Range {
        int min;        
        int max;
        int size;
        
        public Range() {}
        
        public Range(int min, int max, int size) {
            super();
            this.min = min;
            this.max = max;
            this.size = size;
        }
    }

    public Query(String queryName, String entity, String version, Range range, int threads, int loop, int delayMS, boolean withSave) {
        super();
        this.entity = entity;
        this.version = version;
        this.range = range;
        this.threads = threads;
        this.loop = loop;
        this.delayMS = delayMS;
        this.name = queryName;
        this.withSave = withSave;

        Objects.requireNonNull(entity);        
        Objects.requireNonNull(range);
        Objects.requireNonNull(name);
    }

    private String entity, version, name;
    private Range range;
    boolean withSave;

    private int threads, loop, delayMS;
  
    public Range getRange() {
        return range;
    }

    public int getThreads() {
        return threads;
    }

    public int getLoop() {
        return loop;
    }

    public static Query fromProperties(Properties p, String name) {
        String entity = p.getProperty(name + ".entity");
        String version = p.getProperty(name + ".version");
        Range range = new Range();
        range.min = Integer.parseInt(p.getProperty(name + ".range.min", "0"));
        range.max = Integer.parseInt(p.getProperty(name + ".range.max"));
        range.size = Integer.parseInt(p.getProperty(name + ".range.size", "1"));
        int threads = Integer.parseInt(p.getProperty(name + ".threads", "1"));
        int loop = Integer.parseInt(p.getProperty(name + ".loop", "0"));
        int delay = Integer.parseInt(p.getProperty(name + ".delayMS", "1000"));
        boolean withSave = Boolean.parseBoolean(p.getProperty(name+".withSave", "false"));

        return new Query(name, entity, version, range, threads, loop, delay, withSave);
    }
    
    public static List<Query> fromProperties(Properties p) {
        
        return p.keySet().stream()
            .map(Object::toString)
            .map(s -> s.substring(0, s.indexOf('.')))
            .distinct()
            .map(new Function<String, Query>() {

                @Override
                public Query apply(String t) {
                    return Query.fromProperties(p, t);
                }
                
            }).collect(Collectors.toList());                           
    }

    public String getVersion() {
        return version;
    }

    public void setEntity(String entity) {
        this.entity = entity;
    }

    public String getEntity() {
        return entity;
    }

    public int getDelayMS() {
        return delayMS;
    }
    
    @Override
    public String toString() {
        return String.format("%s %s/%s", name, entity, version);
    }

    public String getName() {
        return name;
    }

    public boolean isWithSave() {
        return withSave;
    }

}
