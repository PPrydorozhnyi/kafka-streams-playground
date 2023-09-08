package com.petro.balance.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.petro.balance.model.BalanceStateEvent;
import com.petro.balance.model.BalanceChangeEvent;
import java.time.Instant;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

@Component
public class BalanceProcessor {

  @Autowired
  public void process(
      StreamsBuilder builder,
      @Value("${spring.kafka.topics.in}") String inTopic,
      @Value("${spring.kafka.topics.out}") String outTopic,
      ObjectMapper objectMapper
  ) {

    Serde<BalanceChangeEvent> balanceChangeSerde = new JsonSerde<>(BalanceChangeEvent.class, objectMapper);
    Serde<BalanceStateEvent> balanceStateSerde = new JsonSerde<>(BalanceStateEvent.class, objectMapper);

    builder.stream(inTopic, Consumed.with(Serdes.String(), balanceChangeSerde))
        .filter((key, value) -> value != null && value.balance() > 0)
        .selectKey((key, value) -> value.name(), Named.as("select-key-as-name"))
        .groupByKey(Grouped.<String, BalanceChangeEvent>as("grouped-by-name").withValueSerde(balanceChangeSerde))
        .aggregate(
            () -> new BalanceStateEvent("", 0, Instant.EPOCH, 0),
            this::updateEvent,
            Materialized.<String, BalanceStateEvent, KeyValueStore<Bytes, byte[]>>as("balance-state")
                .withValueSerde(balanceStateSerde)
        )
        .toStream()
        .to(outTopic, Produced.<String, BalanceStateEvent>as("updated-balances")
            .withValueSerde(balanceStateSerde));
  }

  private BalanceStateEvent updateEvent(String name, BalanceChangeEvent update, BalanceStateEvent latestState) {
    final Instant latestUpdateAt = latestState.latestChangeTime();
    final Instant publishedAt = update.publishedAt();
    final long updatedBalance = latestState.balance() + update.balance();
    return new BalanceStateEvent(name,
        updatedBalance,
        latestUpdateAt.isBefore(publishedAt) ? publishedAt : latestUpdateAt,
        latestState.count() + 1);
  }

}
