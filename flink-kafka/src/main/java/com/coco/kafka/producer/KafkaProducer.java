package com.coco.kafka.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.UUID;

@Component
@EnableScheduling
public class KafkaProducer {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private KafkaTemplate kafkaTemplate;

    /**
     * 定时任务
     */
    @Scheduled(cron = "0/5 * * * * ?")
    public void send() {
        String message = UUID.randomUUID().toString();
        ListenableFuture future = kafkaTemplate.send("app_log", "app_log_key", message);
        future.addCallback(success -> logger.info("send-消息发送成功：" + message), failure -> logger.info("消息发送失败：" + message));
    }

}
