package com.petro.balance.config

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.petro.balance.model.ColourBalanceEvent
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import java.util.concurrent.ConcurrentHashMap

@TestConfiguration
class EmbeddedConfiguration(private val properties: KafkaProperties) {
    @Bean
    fun stringFactory(): ProducerFactory<String, String> {
        return DefaultKafkaProducerFactory(
            properties.buildProducerProperties(null),
            StringSerializer(),
            StringSerializer(),
        )
    }

    @Bean
    fun stringKafkaTemplate(stringFactory: ProducerFactory<String, String>): KafkaTemplate<String, String> {
        return KafkaTemplate(stringFactory)
    }

    @Bean
    fun colourBalanceReceiver(objectMapper: ObjectMapper): KafkaColourBalanceListener {
        return KafkaColourBalanceListener(objectMapper)
    }

    class KafkaColourBalanceListener(private val objectMapper: ObjectMapper) {
        private val colourBalances: MutableMap<String, ColourBalanceEvent> = ConcurrentHashMap()

        @KafkaListener(
            groupId = "some-random-group",
            topics = ["\${spring.kafka.topics.balance-colour-topic}"],
            autoStartup = "true",
        )
        fun receive(
            @Header(KafkaHeaders.RECEIVED_KEY) key: String,
            @Payload payload: String?,
        ) {
            try {
                colourBalances[key] = objectMapper.readValue(payload, ColourBalanceEvent::class.java)
            } catch (e: JsonProcessingException) {
                throw RuntimeException(e)
            }
        }

        fun getByName(name: String): ColourBalanceEvent? {
            return colourBalances[name]
        }

        fun clearData() {
            colourBalances.clear()
        }
    }
}
