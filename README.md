# Realtime engine research 2020

I have 6-8 million events/min with no schema. 
And I'm looking for an engine to analyze this stream in near realtime with a low entry barrier (with support for SQL)

So I picked the following engines to consider:
* gpu based: aresdb & mapd(omniscidb)
* kafka derivatives: flink sql (1.12 snapshot) & ksqldb (6.2 master)

## GPU based
Realtime for nowadays is basically cpu bounded. And GPU seems exactly what I need.
There are two promising open sourced engines: mapd and aresdb.
And mapd is a much more production ready for now. While ares is a much simpler to edit as there is extremely small code base.
Both have columnar design and inmemory storage. Both optimized for transfering data to gpu. Both do not support strings. Both super fast on computations.

For aresdb to work I have implemented kafka consumer to produce binary columnar batches. 
For mapd to work I found out that I need to rewrite client communication protocol to avoid huge null values transferring over http.
After that I see mapd (omniscidb) as a top-level engine ready for use on a 10M/min stream if GPU is affordable.

## Kafka based processing
There is a flink which I do not consider as it is a much low level and does not meet my requirements for low entry barrier.
So there is a flink sql in upcoming release. And I tried it. And I needed to patch it to fix deserialization error handling. And still it is too young and not ready for now.

And there is a ksql (wich is called ksqldb now). So I tried it too. And I needed to patch it several times to fix deserialization error handling and bring upcoming feature of suppressing intermidiate group results to reduce stream size on sink. And after all it worked for me. 
It is faster then flink sql, it is much more kafka friendly out of the box, it has schemaless message support, and it is really simple to code.
After all ksql is worth giving a shot in 2020 as a cheap and cheerful engine for realtime stream processing.

# Conclusions
If GPU is not a problem mapd is a complete operational DWH. It has a whole set of database features and ready to use with realtime data.

And surprisingly ksql turned out to be a simple and effective tool for data processing inside kafka infrastructure without need to go hard way with java.

