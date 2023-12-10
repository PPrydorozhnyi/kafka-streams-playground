package com.petro.balance.processor

import com.petro.balance.config.EmbeddedConfiguration
import com.petro.balance.producer.BalanceChangeProducer
import org.awaitility.Awaitility
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.listener.MessageListenerContainer
import org.springframework.kafka.test.EmbeddedKafkaBroker
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.kafka.test.utils.ContainerTestUtils
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.util.ReflectionTestUtils
import java.util.concurrent.TimeUnit
import java.util.function.Consumer

@SpringBootTest
@ActiveProfiles("itest")
@EnableAutoConfiguration
@Import(EmbeddedConfiguration::class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@EmbeddedKafka(topics = [BalanceColourJoinerProcessorTest.PREFERRED_COLOURS_TOPIC], partitions = 3, count = 3)
internal class BalanceColourJoinerProcessorTest {
    @Autowired
    private lateinit var kafkaTemplate: KafkaTemplate<String, String>

    @Autowired
    private lateinit var embeddedKafkaBroker: EmbeddedKafkaBroker

    @Autowired
    private lateinit var registry: KafkaListenerEndpointRegistry

    @Autowired
    private lateinit var colourBalanceReceiver: EmbeddedConfiguration.KafkaColourBalanceListener

    @BeforeEach
    fun setUp() {
        // Wait until the partitions are assigned.
        registry.listenerContainers.forEach(
            Consumer {
                    container: MessageListenerContainer ->
                ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.partitionsPerTopic)
            },
        )
        colourBalanceReceiver.clearData()
    }

    @Test
    fun testKafkaStreams() {
        val name = ReflectionTestUtils.getField(BalanceChangeProducer::class.java, "NAME") as String
        val secondName = ReflectionTestUtils.getField(BalanceChangeProducer::class.java, "SECOND_NAME") as String

        kafkaTemplate.send(PREFERRED_COLOURS_TOPIC, name, COLOUR)

        Awaitility.await().atMost(20, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
            .untilAsserted {
                val petroEvent = colourBalanceReceiver.getByName(name)
                Assertions.assertNotNull(petroEvent)
                Assertions.assertNotEquals(0, petroEvent?.balance)
                Assertions.assertEquals(COLOUR, petroEvent?.colour)
                val secondEvent = colourBalanceReceiver.getByName(secondName)
                Assertions.assertNotNull(secondEvent)
                Assertions.assertNotEquals(0, secondEvent?.balance)
                Assertions.assertNull(secondEvent?.colour)
            }
    }

    companion object {
        const val PREFERRED_COLOURS_TOPIC = "preferred-colours"
        const val COLOUR = "black"
    }
}
