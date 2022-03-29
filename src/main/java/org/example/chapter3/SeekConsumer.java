package org.example.chapter3;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class SeekConsumer {
    public static String brokerList = "localhost:9092";
    public static String topic = "cutecat";
    public static String groupId = "group.cat.ch3.seek";

    public static Properties initConfig() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CatDeserializer.class.getName());
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        return properties;
    }

    public void consume() {
        Properties p = initConfig();
        KafkaConsumer<String, Cat> consumer = new KafkaConsumer<>(p);

        consumer.subscribe(Collections.singletonList(topic));

        Set<TopicPartition> assignment = consumer.assignment();
        while(assignment.size() == 0) {
            consumer.poll(Duration.ofMillis(100));
            assignment = consumer.assignment();
        }

        // 获取了consumer 在当前 topic 所有分区的 start offset 和 end offset
        Map<TopicPartition, Long> startOffsets = consumer.beginningOffsets(assignment);
        for (Map.Entry<TopicPartition, Long> entry: startOffsets.entrySet()) {
            System.out.printf("partition: %s, start offset: %d\n", entry.getKey(), entry.getValue());
        }
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(assignment);
        for (Map.Entry<TopicPartition, Long> entry: endOffsets.entrySet()) {
            System.out.printf("partition: %s, end offset: %d\n", entry.getKey(), entry.getValue());
        }

        // 将消费的起始位置设置成了 9
        for (TopicPartition tp : assignment) {
            consumer.seek(tp, 9);
        }

        // 将消费到的消息打印出来
        ConsumerRecords<String, Cat> records = consumer.poll(Duration.ofMillis(1000));
        System.out.printf("poll 拉取了 %s 条记录\n", records.count());
        for (ConsumerRecord<String, Cat> record : records) {
            Cat c = record.value();
            System.out.printf("receive company: %s\n", c);
        }
    }
}
