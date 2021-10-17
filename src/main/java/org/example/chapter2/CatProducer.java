package org.example.chapter2;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class CatProducer {
    public static String brokerList = "localhost:9092";
    public static String topic = "lazycat";

    public static Properties initConfig() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CompanySerializer.class.getName());
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "producer.client.id.demo");

        return properties;
    }

    public static void send(Company company) {
        Properties prop = initConfig();

        KafkaProducer<String, Company> producer = new KafkaProducer<>(prop);
        ProducerRecord<String, Company> record = new ProducerRecord<>(topic, company);

        try {
            producer.send(record);
            System.out.println("Send Message Success");
        } catch (Exception e) {
            e.printStackTrace();
        }

        producer.close();
    }
}
