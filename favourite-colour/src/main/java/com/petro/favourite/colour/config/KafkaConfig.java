package com.petro.favourite.colour.config;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@RequiredArgsConstructor
@EnableKafkaStreams
public class KafkaConfig {

  @Bean
  NewTopic inTopic(@Value("${spring.kafka.topics.in}") String inTopic) {
    return TopicBuilder.name(inTopic)
        .partitions(3)
        .replicas(1)
        .build();
  }

  @Bean
  NewTopic outTopic(@Value("${spring.kafka.topics.out}") String outTopic) {
    return TopicBuilder.name(outTopic)
        .partitions(3)
        .replicas(1)
        .build();
  }

}
