package com.petro.balance.config

import com.fasterxml.jackson.databind.ObjectMapper
import com.petro.balance.model.BalanceChangeEvent
import lombok.RequiredArgsConstructor
import org.apache.kafka.clients.admin.NewTopic
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafkaStreams
import org.springframework.kafka.config.TopicBuilder
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.support.serializer.JsonSerializer

@Configuration
@EnableKafkaStreams
@RequiredArgsConstructor
class KafkaConfig {
    @Bean
    fun inTopic(
        @Value("\${spring.kafka.topics.in}") inTopic: String,
    ): NewTopic {
        return TopicBuilder.name(inTopic)
            .partitions(3)
            .replicas(1)
            .build()
    }

    @Bean
    fun outTopic(
        @Value("\${spring.kafka.topics.out}") outTopic: String,
    ): NewTopic {
        return TopicBuilder.name(outTopic)
            .partitions(3)
            .replicas(1)
            .build()
    }

    @Bean
    fun balanceColourTopic(
        @Value("\${spring.kafka.topics.balance-colour-topic}") outTopic: String,
    ): NewTopic {
        return TopicBuilder.name(outTopic)
            .partitions(3)
            .replicas(1)
            .build()
    }

    @Bean
    fun resultProducerConfigFactory(
        kafkaProperties: KafkaProperties,
        objectMapper: ObjectMapper,
    ): ProducerFactory<String, BalanceChangeEvent> {
        val valueSerializer = JsonSerializer<BalanceChangeEvent>(objectMapper)
        return DefaultKafkaProducerFactory(kafkaProperties.buildProducerProperties(null), null, valueSerializer)
    }

    @Bean
    fun resultKafkaTemplate(
        resultProducerConfigFactory: ProducerFactory<String, BalanceChangeEvent>,
    ): KafkaTemplate<String, BalanceChangeEvent> {
        val kafkaTemplate = KafkaTemplate(resultProducerConfigFactory)
        kafkaTemplate.setObservationEnabled(true)
        return kafkaTemplate
    }
}
