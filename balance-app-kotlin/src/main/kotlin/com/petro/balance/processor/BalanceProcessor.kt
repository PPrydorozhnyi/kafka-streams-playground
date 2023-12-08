package com.petro.balance.processor

import com.fasterxml.jackson.databind.ObjectMapper
import com.petro.balance.model.BalanceChangeEvent
import com.petro.balance.model.BalanceStateEvent
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Grouped
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Named
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.state.KeyValueStore
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.support.serializer.JsonSerde
import org.springframework.stereotype.Component
import java.time.Instant

@Component
class BalanceProcessor {
    @Autowired
    fun buildPipeline(
        builder: StreamsBuilder,
        @Value("\${spring.kafka.topics.in}") inTopic: String?,
        @Value("\${spring.kafka.topics.out}") outTopic: String?,
        objectMapper: ObjectMapper?,
    ) {
        val balanceChangeSerde: Serde<BalanceChangeEvent> = JsonSerde(BalanceChangeEvent::class.java, objectMapper)
        val balanceStateSerde: Serde<BalanceStateEvent> = JsonSerde(BalanceStateEvent::class.java, objectMapper)

        builder.stream(inTopic, Consumed.with(Serdes.String(), balanceChangeSerde))
            .filter { _: String?, value: BalanceChangeEvent? -> value?.balance?.let { it > 0 } ?: false }
            .selectKey({ _: String?, value: BalanceChangeEvent -> value.name }, Named.`as`("select-key-as-name"))
            .groupByKey(Grouped.`as`<String, BalanceChangeEvent>("grouped-by-name").withValueSerde(balanceChangeSerde))
            .aggregate(
                { BalanceStateEvent("", 0, Instant.EPOCH, 0) },
                {
                        _: String,
                        update: BalanceChangeEvent,
                        latestState: BalanceStateEvent,
                    ->
                    updateEvent(update, latestState)
                },
                Materialized.`as`<String, BalanceStateEvent, KeyValueStore<Bytes, ByteArray>>("balance-state")
                    .withValueSerde(balanceStateSerde),
            )
            .toStream()
            .to(
                outTopic,
                Produced.`as`<String, BalanceStateEvent>("updated-balances")
                    .withValueSerde(balanceStateSerde),
            )
    }

    private fun updateEvent(
        update: BalanceChangeEvent,
        latestState: BalanceStateEvent,
    ): BalanceStateEvent {
        val latestUpdateAt = latestState.latestChangeTime
        val publishedAt = update.publishedAt

        return latestState.copy(
            name = update.name,
            balance = latestState.balance + update.balance,
            latestChangeTime = if (latestUpdateAt < publishedAt) publishedAt else latestUpdateAt,
            count = latestState.count + 1,
        )
    }
}
