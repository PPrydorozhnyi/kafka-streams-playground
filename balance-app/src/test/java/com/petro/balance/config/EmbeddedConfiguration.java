package com.petro.balance.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.petro.balance.model.ColourBalanceEvent;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;

@TestConfiguration
public class EmbeddedConfiguration {

    @Autowired
    private KafkaProperties properties;

    @Bean
    public ProducerFactory<String, String> stringFactory() {
        return new DefaultKafkaProducerFactory<>(
                properties.buildProducerProperties(null), new StringSerializer(), new StringSerializer());
    }

    @Bean
    public KafkaTemplate<String, String> stringKafkaTemplate(ProducerFactory<String, String> stringFactory) {
        return new KafkaTemplate<>(stringFactory);
    }

    @Bean
    public KafkaColourBalanceListener colourBalanceReceiver(ObjectMapper objectMapper) {
        return new KafkaColourBalanceListener(objectMapper);
    }

    public static class KafkaColourBalanceListener {
        private final Map<String, ColourBalanceEvent> colourBalances = new ConcurrentHashMap<>();
        private final ObjectMapper objectMapper;

        public KafkaColourBalanceListener(ObjectMapper objectMapper) {
            this.objectMapper = objectMapper;
        }

        @KafkaListener(
                groupId = "some-random-group",
                topics = "${spring.kafka.topics.balance-colour-topic}",
                autoStartup = "true")
        void receive(@Header(KafkaHeaders.RECEIVED_KEY) String key, @Payload final String payload) {
            try {
                colourBalances.put(key, objectMapper.readValue(payload, ColourBalanceEvent.class));
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }

        public ColourBalanceEvent getByName(String name) {
            return colourBalances.get(name);
        }

        public void clearData() {
            colourBalances.clear();
        }
    }
}
