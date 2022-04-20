package org.example.chapter3;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.Properties;

public class CatProducer {
    private final String brokerList;
    private final String topic;

    public CatProducer(String brokerList, String topic) {
        this.brokerList = brokerList;
        this.topic = topic;
    }

    public Properties initConfig() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokerList);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "producer.client.id.ch3");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CatSerializer.class.getName());

        return properties;
    }

    public void send(Cat cat, Integer partition) {
        Properties prop = initConfig();

        KafkaProducer<String, Cat> producer = new KafkaProducer<>(prop);
        ProducerRecord<String, Cat> record = new ProducerRecord<>(topic, partition, "key", cat);

        try {
            producer.send(record);
            System.out.printf("Send cat %s to %s Success\n", cat.getName(), partition);
        } catch (Exception e) {
            e.printStackTrace();
        }

        producer.close();
    }

    public void send(ArrayList<Cat> cats) {
        Properties prop = initConfig();

        KafkaProducer<String, Cat> producer = new KafkaProducer<>(prop);
        for (Cat cat : cats) {
            ProducerRecord<String, Cat> record = new ProducerRecord<>(topic, "key", cat);
            try {
                producer.send(record);
                System.out.printf("Send cat %s Success\n", cat.getName());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        producer.close();
    }
}
