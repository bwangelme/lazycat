package org.example.chapter3;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producer {
    public static String brokerList = "localhost:9092";
    public static String topic = "cutecat";

    public static Properties initConfig() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "producer.client.id.ch3");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CatSerializer.class.getName());

        return properties;
    }

    public static void send(Cat cat, Integer partition) {
        Properties prop = initConfig();

        KafkaProducer<String, Cat> producer = new KafkaProducer<>(prop);
        ProducerRecord<String, Cat> record = new ProducerRecord<>(topic, partition, "key", cat);

        try {
            producer.send(record);
            System.out.printf("Send cat %s Success\n", cat.getName());
        } catch (Exception e) {
            e.printStackTrace();
        }

        producer.close();
    }
}
