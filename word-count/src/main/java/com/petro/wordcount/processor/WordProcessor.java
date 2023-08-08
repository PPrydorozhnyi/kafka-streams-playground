package com.petro.wordcount.processor;

import java.util.Arrays;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class WordProcessor {

  @Autowired
  public void process(
      StreamsBuilder builder,
      @Value("${spring.kafka.topics.in}") String inTopic,
      @Value("${spring.kafka.topics.out}") String outTopic
      ) {

    // 1 - stream from Kafka
    final KStream<String, String> textLines = builder.stream(inTopic);

    final KTable<String, Long> wordCounts = textLines
        // 2 - map values to lowercase
        .mapValues(textLine -> textLine.toLowerCase())
        // 3 - flatmap values split by space
        .flatMapValues(textLine -> Arrays.asList(textLine.split("\\W+")))
        // 4 - select key to apply a key (we discard the old key)
        .selectKey((key, word) -> word)
        // 5 - group by key before aggregation
        .groupByKey()
        // 6 - count occurrences
        .count(Materialized.as("Counts"));

    // 7 - to in order to write the results back to kafka
    wordCounts.toStream()
        .mapValues(Object::toString)
        .to(outTopic);
  }

}
