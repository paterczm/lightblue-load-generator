# Lightblue-load-generator

A simple load generator for lightblue. It reads and saves data in random ranges, causing lots of cache misses in mongo. This is similar to what production load may look like.

## Usage

```
java -jar lightblue-load-generator-1.0-SNAPSHOT-jar-with-dependencies.jar
usage: LightblueLoadGenerator
 -h,--help                                              prints usage
 -lc,--lightblue-client <lightblue-client.properties>   Configuration file for lightblue-client
 -q,--queries <queries.properties>                      Property files with queries, merged in order provided
    --run-for-minutes <arg>                             Ignores the loop setting and runs for specified number of minutes
    --run-forever                                       Ignores the loop setting and runs forever
 -s,--stats                                             Enables stats. Note: stats consume resources.
    --stats-delay <minutes>                             How often generate stats
```

## Defining queries

Example query definition in a property file:
```
# The name of the query is user500
user500.entity=user # Entity to query
user500.version=0.0.3 # Version of the entity to query
user500.range.min=0 # Minimum range
user500.range.max=7000000 # Maximum range, i.e. you have 7 mi users
user500.range.size=500 # Range size, i.e. you want to read users 500 at a time
user500.range.isIdRange=true # If true, _id range is used to read batches of users (so has to be numeric). If false, a range is used.
user500.range.idField=_id # name of the field to use for selecting ranges (when isIdRange=true)
user500.loop=50 # How many times to run the query per thread
user500.threads=10 # How many requests to lightblue in parallel
user500.withSave=false # After reading the data, should it be saved back to lightblue to simulate writes?
```

You can pass more than one query file to the generator. They will be merged in order provided. It allows to decouple the query definition from load definition (threads, loops).

### isIdRange

isIdRange = true example:
```
POST /find/user/0.0.3, body: {"query":{"$and":[{"field":"_id","op":">=","rvalue":685381421},{"field":"_id","op":"<=","rvalue":685381423}]},"projection":{"field":"*","include":true,"recursive":true}}
```
isIdRange = false example:
```
POST /find/user/0.0.3, body: {"range":[685381421,685381423],"projection":{"field":"*","include":true,"recursive":true}}
```

Reading ranges of data means that db cursor will need to "walk" all the way from zero to the end of the range. For large collections this can be very slow and is not similar to real world scenarios. _id ranges are preferred, however, that requires the _id to be a sequence.

# Debugging
Pass ```-Dorg.slf4j.simpleLogger.defaultLogLevel=debug``` to jvm to see the actual requests generated and lightblue responses.
