package org.example.chapter3;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * 这个程序证明，消费者提交的 offset = 消费者拉取到的 offset + 1
 */
public class Consumer {
    public static String brokerList = "localhost:9092";
    public static String topic = "cutecat";
    public static String groupId = "group.cat.ch3";

    public static Properties initConfig() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CatDeserializer.class.getName());
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // 关闭自动提交 offset
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        return properties;
    }

    public void consume() {
        Properties p = initConfig();

        KafkaConsumer<String, Cat> consumer = new KafkaConsumer<>(p);
        TopicPartition tp = new TopicPartition(topic, 1);
        consumer.assign(Arrays.asList(tp));
        long lastConsumedOffset = -1;

        while (true) {
            ConsumerRecords<String, Cat> records = consumer.poll(Duration.ofMillis(1000));
            System.out.printf("poll 拉取了 %s 条记录\n", records.count());
            if (records.isEmpty()) {
                break;
            }

            List<ConsumerRecord<String, Cat>> partitionRecords = records.records(tp);
            // 获取最后一条记录的 offset
            lastConsumedOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
            // 同步提交 offset
            consumer.commitSync();

            System.out.printf("消费者达到的 offset %d\n", lastConsumedOffset);
            OffsetAndMetadata offsetAndMetadata = consumer.committed(tp);
            System.out.printf("消费者提交的 offset %d\n", offsetAndMetadata.offset());
            long position = consumer.position(tp);
            System.out.printf("消费者下次拉取时的开始位置 %d\n", position);
        }
    }
}
