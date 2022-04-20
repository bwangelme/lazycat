package org.example.chapter4;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.chapter3.Cat;
import org.example.chapter3.CatDeserializer;

import java.time.Duration;
import java.util.*;

public class CuteCatRangerConsumer {
    public static String brokerList = "localhost:9092";
    public static String topic = "angrycat";
    public static String groupId = "ranger.consumer";

    public static Properties initConfig() {
        Properties prop = new Properties();
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CatDeserializer.class.getName());
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        return prop;
    }

    public static void consume() {
        Properties p = initConfig();
        KafkaConsumer<String, Cat> consumer = new KafkaConsumer<>(p);

        consumer.subscribe(Collections.singletonList(topic));

        while (true) {
            ConsumerRecords<String, Cat> records = consumer.poll(Duration.ofMillis(5000));
            System.out.printf("poll 拉取了 %s 条记录\n", records.count());

            for (ConsumerRecord<String, Cat> record : records) {
                Cat c = record.value();
                System.out.printf("receive cat: %s @ partition %s\n", c, record.partition());
            }
        }
    }
}
