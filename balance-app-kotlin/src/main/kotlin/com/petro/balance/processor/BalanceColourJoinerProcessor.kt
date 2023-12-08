package com.petro.balance.processor

import com.fasterxml.jackson.databind.ObjectMapper
import com.petro.balance.model.BalanceStateEvent
import com.petro.balance.model.ColourBalanceEvent
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Joined
import org.apache.kafka.streams.kstream.Produced
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.support.serializer.JsonSerde
import org.springframework.stereotype.Component

@Component
class BalanceColourJoinerProcessor {
    @Autowired
    fun process(
        builder: StreamsBuilder,
        @Value("\${spring.kafka.topics.out}") balanceStateTopic: String,
        @Value("\${spring.kafka.topics.colour-topic}") colourTopic: String,
        @Value("\${spring.kafka.topics.balance-colour-topic}") outTopic: String,
        objectMapper: ObjectMapper,
    ) {
        val balanceStateSerde: Serde<BalanceStateEvent> = JsonSerde(BalanceStateEvent::class.java, objectMapper)
        val colourBalanceSerde: Serde<ColourBalanceEvent> = JsonSerde(ColourBalanceEvent::class.java, objectMapper)

        val colourPreferenceTable = builder.table<String, String>(colourTopic)

        val balanceUpdatesStream =
            builder.stream(
                balanceStateTopic,
                Consumed.with(
                    Serdes.String(),
                    balanceStateSerde,
                ),
            )

        balanceUpdatesStream.leftJoin(
            colourPreferenceTable,
            { balance: BalanceStateEvent?, colour: String? -> ColourBalanceEvent(colour, balance?.balance ?: 0) },
            Joined.`as`<String, BalanceStateEvent, String>("colour-balance-join").withValueSerde(balanceStateSerde),
        ).to(outTopic, Produced.`as`<String, ColourBalanceEvent>("join-producer").withValueSerde(colourBalanceSerde))
    }
}
