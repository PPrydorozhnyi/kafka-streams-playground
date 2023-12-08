package com.petro.balance.producer;

import com.petro.balance.model.BalanceChangeEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class BalanceChangeProducer {

    private static final String NAME = "Petro";
    private static final String SECOND_NAME = "Jack";

    private final KafkaTemplate<String, BalanceChangeEvent> kafkaTemplate;

    @Value("${spring.kafka.topics.in}")
    private final String inTopic;

    @Scheduled(fixedDelay = 20_000)
    public void publishBalanceChange() {
        sendMessage(NAME);
        sendMessage(SECOND_NAME);
    }

    private void sendMessage(String name) {
        kafkaTemplate.send(inTopic, new BalanceChangeEvent(name)).whenCompleteAsync((event, e) -> {
            if (e == null) {
                log.debug("Published event {} successfully for {}", event, NAME);
            } else {
                log.error("Failed to send balance update message for {}", NAME, e);
            }
        });
    }
}
