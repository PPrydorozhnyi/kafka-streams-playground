package com.petro.balance.producer

import com.petro.balance.model.BalanceChangeEvent
import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.SendResult
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

@Component
class BalanceChangeProducer(
    private val kafkaTemplate: KafkaTemplate<String, BalanceChangeEvent>,
    @Value("\${spring.kafka.topics.in}") private val inTopic: String,
) {
    @Scheduled(fixedDelay = 20000)
    fun publishBalanceChange() {
        sendMessage(NAME)
        sendMessage(SECOND_NAME)
    }

    private fun sendMessage(name: String) {
        kafkaTemplate.send(inTopic, BalanceChangeEvent(name))
            .whenCompleteAsync { event: SendResult<String, BalanceChangeEvent>?, e: Throwable? ->
                if (e == null) {
                    log.debug { "Published event $event successfully for $NAME" }
                } else {
                    log.error(e) { "Failed to send balance update message for $NAME" }
                }
            }
    }

    companion object {
        private const val NAME = "Petro"
        private const val SECOND_NAME = "Jack"
        private val log = KotlinLogging.logger {}
    }
}
