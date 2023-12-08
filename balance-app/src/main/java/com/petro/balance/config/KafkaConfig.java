package com.petro.balance.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.petro.balance.model.BalanceChangeEvent;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

@Configuration
@EnableKafkaStreams
@RequiredArgsConstructor
public class KafkaConfig {

    @Bean
    NewTopic inTopic(@Value("${spring.kafka.topics.in}") String inTopic) {
        return TopicBuilder.name(inTopic).partitions(3).replicas(1).build();
    }

    @Bean
    NewTopic outTopic(@Value("${spring.kafka.topics.out}") String outTopic) {
        return TopicBuilder.name(outTopic).partitions(3).replicas(1).build();
    }

    @Bean
    NewTopic balanceColourTopic(@Value("${spring.kafka.topics.balance-colour-topic}") String outTopic) {
        return TopicBuilder.name(outTopic).partitions(3).replicas(1).build();
    }

    @Bean
    public ProducerFactory<String, BalanceChangeEvent> resultProducerConfigFactory(
            KafkaProperties kafkaProperties, ObjectMapper objectMapper) {
        final var valueSerializer = new JsonSerializer<BalanceChangeEvent>(objectMapper);
        return new DefaultKafkaProducerFactory<>(kafkaProperties.buildProducerProperties(null), null, valueSerializer);
    }

    @Bean
    public KafkaTemplate<String, BalanceChangeEvent> resultkafkaTemplate(
            ProducerFactory<String, BalanceChangeEvent> resultProducerConfigFactory) {
        var kafkaTemplate = new KafkaTemplate<>(resultProducerConfigFactory);
        kafkaTemplate.setObservationEnabled(true);
        return kafkaTemplate;
    }
}
