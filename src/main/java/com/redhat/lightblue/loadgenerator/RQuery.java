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
public class RQuery {

    public static class Range {
        int min;
        int max;
        int size;
        boolean isIdRange;
        String idField;

        public Range() {
        }

        public Range(int min, int max, int size, boolean isIdRange, String idField) {
            super();
            this.min = min;
            this.max = max;
            this.size = size;
            this.isIdRange = isIdRange;
            this.idField = idField;
        }


    }

    public RQuery(String queryName, String entity, String version, Range range, int threads, int loop, int delayMS, boolean withSave, boolean ignore) {
        super();
        this.entity = entity;
        this.version = version;
        this.range = range;
        this.threads = threads;
        this.loop = loop;
        this.delayMS = delayMS;
        this.name = queryName;
        this.withSave = withSave;
        this.ignore = ignore;

        Objects.requireNonNull(entity);
        Objects.requireNonNull(range);
        Objects.requireNonNull(name);
        if (range.isIdRange)
            Objects.requireNonNull(range.idField);
    }

    private String entity, version, name;
    private Range range;
    private boolean withSave, ignore;

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

    public static RQuery fromProperties(Properties p, String name) {
        String entity = p.getProperty(name + ".entity");
        String version = p.getProperty(name + ".version");
        Range range = new Range();
        range.min = Integer.parseInt(p.getProperty(name + ".range.min", "0"));
        range.max = Integer.parseInt(p.getProperty(name + ".range.max"));
        range.size = Integer.parseInt(p.getProperty(name + ".range.size", "1"));
        range.isIdRange = Boolean.parseBoolean(p.getProperty(name + ".range.isIdRange", "false"));
        range.idField = p.getProperty(name + ".range.idField");
        int threads = Integer.parseInt(p.getProperty(name + ".threads"));
        int loop = Integer.parseInt(p.getProperty(name + ".loop", "0"));
        int delay = Integer.parseInt(p.getProperty(name + ".delayMS", "100"));
        boolean withSave = Boolean.parseBoolean(p.getProperty(name + ".withSave", "false"));
        boolean ignore = Boolean.parseBoolean(p.getProperty(name + ".ignore", "false"));

        return new RQuery(name, entity, version, range, threads, loop, delay, withSave, ignore);
    }

    public static List<RQuery> fromProperties(Properties p) {

        return p.keySet().stream().map(Object::toString).map(s -> s.substring(0, s.indexOf('.'))).distinct().map(new Function<String, RQuery>() {

            @Override
            public RQuery apply(String t) {
                return RQuery.fromProperties(p, t);
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

    public boolean isIgnore() {
        return ignore;
    }

}
