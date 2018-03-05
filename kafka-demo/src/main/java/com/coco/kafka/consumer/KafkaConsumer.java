package com.coco.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;

public class KafkaConsumer {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @KafkaListener(topics = {"app_log"})
    public void listen(ConsumerRecord<?, ?> record) {
        logger.info("KafkaListener -> kafka的key: {}, kafka的value: {}", record.key(), record.value());
    }

}
