package com.petro.balance.topology;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.petro.balance.config.CommonConfig;
import com.petro.balance.model.BalanceChangeEvent;
import com.petro.balance.model.BalanceStateEvent;
import com.petro.balance.processor.BalanceProcessor;
import com.petro.balance.producer.BalanceChangeProducer;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.test.util.ReflectionTestUtils;

class BalanceProcessorTopologyTest {

  private static final String IN_TOPIC = "in";
  private static final String OUT_TOPIC = "out";

  @Test
  void balanceTopologyTest() {
    StreamsBuilder streamsBuilder = new StreamsBuilder();

    final BalanceProcessor balanceProcessor = new BalanceProcessor();
    final ObjectMapper objectMapper = new CommonConfig().objectMapper();
    balanceProcessor.buildPipeline(streamsBuilder, IN_TOPIC, OUT_TOPIC, objectMapper);

    Topology topology = streamsBuilder.build();
    Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

    Serde<BalanceChangeEvent> balanceChangeSerde = new JsonSerde<>(BalanceChangeEvent.class, objectMapper);
    Serde<BalanceStateEvent> balanceStateSerde = new JsonSerde<>(BalanceStateEvent.class, objectMapper);

    try (TopologyTestDriver topologyTestDriver = new TopologyTestDriver(topology, streamsConfiguration)) {
      TestInputTopic<String, BalanceChangeEvent> inputTopic = topologyTestDriver
          .createInputTopic(IN_TOPIC, new StringSerializer(), balanceChangeSerde.serializer());

      TestOutputTopic<String, BalanceStateEvent> outputTopic = topologyTestDriver
          .createOutputTopic(OUT_TOPIC, new StringDeserializer(), balanceStateSerde.deserializer());

      final var name = (String) ReflectionTestUtils.getField(BalanceChangeProducer.class, "NAME");
      final var firstEvent = new BalanceChangeEvent(name, 10, Instant.ofEpochMilli(System.currentTimeMillis()));
      final var secondEvent = new BalanceChangeEvent(name, 20, Instant.EPOCH);
      final var thirdEvent = new BalanceChangeEvent(name, 40, Instant.ofEpochMilli(System.currentTimeMillis()).plusSeconds(30));
      final var fourthEvent = new BalanceChangeEvent(name, -5, Instant.EPOCH);

      inputTopic.pipeValueList(List.of(firstEvent, secondEvent, thirdEvent, fourthEvent));

      Assertions.assertIterableEquals(
          List.of(
              KeyValue.pair(name, new BalanceStateEvent(name, 10, firstEvent.publishedAt(), 1)),
              KeyValue.pair(name, new BalanceStateEvent(name, 30, firstEvent.publishedAt(), 2)),
              KeyValue.pair(name, new BalanceStateEvent(name, 70, thirdEvent.publishedAt(), 3))
          ),
          outputTopic.readKeyValuesToList());
    }
  }
}
