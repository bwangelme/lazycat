package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class CatProducer {
    public static String brokerList = "localhost:9092";
    public static String topic = "lazycat";

    public static void send(String msg) {
        Properties properties = new Properties();
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("bootstrap.servers", brokerList);

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, msg);

        try {
            producer.send(record);
            System.out.println("Send Message Success");
        } catch (Exception e) {
            e.printStackTrace();
        }

        producer.close();
    }
}
