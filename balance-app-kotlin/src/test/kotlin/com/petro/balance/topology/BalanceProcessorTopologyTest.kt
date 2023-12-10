package com.petro.balance.topology

import com.petro.balance.config.CommonConfig
import com.petro.balance.model.BalanceChangeEvent
import com.petro.balance.model.BalanceStateEvent
import com.petro.balance.processor.BalanceProcessor
import com.petro.balance.producer.BalanceChangeProducer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TopologyTestDriver
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.springframework.kafka.support.serializer.JsonSerde
import org.springframework.test.util.ReflectionTestUtils
import java.time.Instant
import java.util.Properties

internal class BalanceProcessorTopologyTest {
    @Test
    fun balanceTopologyTest() {
        val streamsBuilder = StreamsBuilder()
        val balanceProcessor = BalanceProcessor()
        val objectMapper = CommonConfig().objectMapper()

        balanceProcessor.buildPipeline(streamsBuilder, IN_TOPIC, OUT_TOPIC, objectMapper)
        val topology = streamsBuilder.build()

        val streamsConfiguration = Properties()
        streamsConfiguration[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass.getName()
        streamsConfiguration[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String().javaClass.getName()

        val balanceChangeSerde: Serde<BalanceChangeEvent> = JsonSerde(BalanceChangeEvent::class.java, objectMapper)
        val balanceStateSerde: Serde<BalanceStateEvent> = JsonSerde(BalanceStateEvent::class.java, objectMapper)

        TopologyTestDriver(topology, streamsConfiguration).use { topologyTestDriver ->
            val inputTopic =
                topologyTestDriver
                    .createInputTopic(IN_TOPIC, StringSerializer(), balanceChangeSerde.serializer())
            val outputTopic =
                topologyTestDriver
                    .createOutputTopic(OUT_TOPIC, StringDeserializer(), balanceStateSerde.deserializer())

            val name = ReflectionTestUtils.getField(BalanceChangeProducer::class.java, "NAME") as String

            val firstEvent = BalanceChangeEvent(name, 10, Instant.ofEpochMilli(System.currentTimeMillis()))
            val secondEvent = BalanceChangeEvent(name, 20, Instant.EPOCH)
            val thirdEvent =
                BalanceChangeEvent(name, 40, Instant.ofEpochMilli(System.currentTimeMillis()).plusSeconds(30))
            val fourthEvent = BalanceChangeEvent(name, -5, Instant.EPOCH)

            inputTopic.pipeValueList(listOf(firstEvent, secondEvent, thirdEvent, fourthEvent))

            Assertions.assertIterableEquals(
                listOf(
                    KeyValue.pair(name, BalanceStateEvent(name, 10, firstEvent.publishedAt, 1)),
                    KeyValue.pair(name, BalanceStateEvent(name, 30, firstEvent.publishedAt, 2)),
                    KeyValue.pair(name, BalanceStateEvent(name, 70, thirdEvent.publishedAt, 3)),
                ),
                outputTopic.readKeyValuesToList(),
            )
        }
    }

    companion object {
        private const val IN_TOPIC = "in"
        private const val OUT_TOPIC = "out"
    }
}
