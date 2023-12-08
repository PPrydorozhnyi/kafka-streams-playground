package com.petro.balance.processor;

import com.petro.balance.config.EmbeddedConfiguration;
import com.petro.balance.model.ColourBalanceEvent;
import com.petro.balance.producer.BalanceChangeProducer;
import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.util.ReflectionTestUtils;

@SpringBootTest
@ActiveProfiles("itest")
@EnableAutoConfiguration
@Import(EmbeddedConfiguration.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@EmbeddedKafka(
        topics = {BalanceColourJoinerProcessorTest.PREFERRED_COLOURS_TOPIC},
        partitions = 3,
        count = 3)
class BalanceColourJoinerProcessorTest {

    public static final String PREFERRED_COLOURS_TOPIC = "preferred-colours";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    @Autowired
    private EmbeddedConfiguration.KafkaColourBalanceListener colourBalanceReceiver;

    @BeforeEach
    public void setUp() {
        // Wait until the partitions are assigned.
        registry.getListenerContainers()
                .forEach(container ->
                        ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic()));

        colourBalanceReceiver.clearData();
    }

    @Test
    void testKafkaStreams() {
        final var name = (String) ReflectionTestUtils.getField(BalanceChangeProducer.class, "NAME");
        final var secondName = (String) ReflectionTestUtils.getField(BalanceChangeProducer.class, "SECOND_NAME");
        final var colour = "black";

        kafkaTemplate.send(PREFERRED_COLOURS_TOPIC, name, colour);

        Awaitility.await()
                .atMost(20, TimeUnit.SECONDS)
                .pollDelay(100, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    final ColourBalanceEvent petroEvent = colourBalanceReceiver.getByName(name);
                    Assertions.assertNotNull(petroEvent);
                    Assertions.assertNotEquals(0, petroEvent.balance());
                    Assertions.assertEquals(colour, petroEvent.colour());

                    final ColourBalanceEvent secondEvent = colourBalanceReceiver.getByName(secondName);
                    Assertions.assertNotNull(secondEvent);
                    Assertions.assertNotEquals(0, secondEvent.balance());
                    Assertions.assertNull(secondEvent.colour());
                });
    }
}
