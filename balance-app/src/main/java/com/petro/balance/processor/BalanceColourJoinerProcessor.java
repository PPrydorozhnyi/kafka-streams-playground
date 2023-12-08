package com.petro.balance.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.petro.balance.model.BalanceStateEvent;
import com.petro.balance.model.ColourBalanceEvent;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

@Component
public class BalanceColourJoinerProcessor {

    @Autowired
    public void process(
            StreamsBuilder builder,
            @Value("${spring.kafka.topics.out}") String balanceStateTopic,
            @Value("${spring.kafka.topics.colour-topic}") String colourTopic,
            @Value("${spring.kafka.topics.balance-colour-topic}") String outTopic,
            ObjectMapper objectMapper) {

        Serde<BalanceStateEvent> balanceStateSerde = new JsonSerde<>(BalanceStateEvent.class, objectMapper);
        Serde<ColourBalanceEvent> colourBalanceSerde = new JsonSerde<>(ColourBalanceEvent.class, objectMapper);

        final KTable<String, String> colourPreferenceTable = builder.table(colourTopic);

        final KStream<String, BalanceStateEvent> balanceUpdatesStream =
                builder.stream(balanceStateTopic, Consumed.with(Serdes.String(), balanceStateSerde));

        balanceUpdatesStream
                .leftJoin(
                        colourPreferenceTable,
                        (balance, colour) -> new ColourBalanceEvent(colour, balance == null ? 0 : balance.balance()),
                        Joined.<String, BalanceStateEvent, String>as("colour-balance-join")
                                .withValueSerde(balanceStateSerde))
                .to(
                        outTopic,
                        Produced.<String, ColourBalanceEvent>as("join-producer").withValueSerde(colourBalanceSerde));
    }
}
