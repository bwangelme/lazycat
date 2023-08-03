package org.example.chapter4;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor;
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

    /**
     * kafka 消费消息的默认策略是顺序遍历所有分区 RangeAssignor
     * <p>
     * poll 拉取了 1 条记录
     * receive cat: Cat(name=大橘0号, kind=angry) @ partition 0
     * poll 拉取了 1 条记录
     * receive cat: Cat(name=大橘1号, kind=angry) @ partition 1
     * poll 拉取了 1 条记录
     * receive cat: Cat(name=大橘2号, kind=angry) @ partition 2
     * poll 拉取了 1 条记录
     * receive cat: Cat(name=大橘3号, kind=angry) @ partition 3
     * poll 拉取了 1 条记录
     * receive cat: Cat(name=大橘4号, kind=angry) @ partition 4
     * poll 拉取了 1 条记录
     * receive cat: Cat(name=大橘5号, kind=angry) @ partition 0
     * poll 拉取了 1 条记录
     * receive cat: Cat(name=大橘6号, kind=angry) @ partition 1
     * poll 拉取了 1 条记录
     * receive cat: Cat(name=大橘7号, kind=angry) @ partition 2
     * poll 拉取了 1 条记录
     * receive cat: Cat(name=大橘8号, kind=angry) @ partition 3
     * poll 拉取了 1 条记录
     * receive cat: Cat(name=大橘9号, kind=angry) @ partition 4
     */
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

    public static void round_robin_consume() {
        // TODO: 它的消费策略是怎样的，而且需要用多个 consumer 来验证
        Properties p = initConfig();
        PartitionAssignor assignmentStrategy = new RoundRobinAssignor();
        p.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, assignmentStrategy);

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
