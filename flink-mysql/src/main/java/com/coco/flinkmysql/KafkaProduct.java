package com.coco.flinkmysql;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.commons.lang3.RandomStringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;
import java.util.Random;

@Configuration
public class KafkaProduct {

    @Value("${kafka.hosts}")
    static String broker = "localhost:9092";
    @Value("${kafka.topic}")
    static String topic = "mytopic";

    public static void main(String[] args) throws InterruptedException {
        System.out.println("broker:" + broker + ", topic:" + topic);

        Properties props = new Properties();
        props.put("metadata.broker.list", broker);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");

        ProducerConfig pConfig = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(pConfig);

        int count = 10;
        Random random = new Random();
        for (int i = 0; i < count; ++i) {
            String json = random.nextInt(10) + ":" + RandomStringUtils.randomAlphabetic(3) + ":" + random.nextInt(1000);
            producer.send(new KeyedMessage<String, String>(topic, json));
            Thread.sleep(1000);
            System.err.println("第" + i + "条数据已经发送 -> " + json);
        }
    }

}
