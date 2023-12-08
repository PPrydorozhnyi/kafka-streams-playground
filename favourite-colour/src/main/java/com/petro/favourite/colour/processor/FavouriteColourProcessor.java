package com.petro.favourite.colour.processor;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class FavouriteColourProcessor {

    private static final String SEPARATOR = ",";

    @Autowired
    public void process(
            StreamsBuilder builder,
            @Value("${spring.kafka.topics.in}") String inTopic,
            @Value("${spring.kafka.topics.out}") String outTopic) {

        // input example
        // st, blue
        // something, red
        // 1 - stream from Kafka
        final KTable<String, Long> coloursCounts = builder.<String, String>stream(inTopic)
                // 2 - filter correct
                .filter((key, value) -> value.split(SEPARATOR).length == 2)
                // 3 - select key to apply a key (we discard the old key)
                .map((key, value) -> {
                    final String[] splitBody = value.split(SEPARATOR);
                    return KeyValue.pair(splitBody[0], splitBody[1]);
                })
                // 5 - map to table to persist
                .toTable(Materialized.as("user-preference"))
                // 6 - group by key to run aggregation function
                .groupBy((key, value) -> KeyValue.pair(value, value))
                // 7 - actual counting operation
                .count(Materialized.as("count-by-colour"));

        // 8 - to in order to write the results back to kafka in string format
        coloursCounts.toStream().mapValues(Object::toString).to(outTopic);
    }
}
