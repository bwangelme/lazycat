package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Hello world!
 */
public class App {
    public static final String brokerList = "localhost:9092";
    public static final String topic = "lazycat";

    public static void main(String[] args) {
        String kind = args[0];
        if (kind.equals("producer")) {
            CatProducer producer = new CatProducer();
            producer.send("Hello, World");
        } else if (kind.equals("consumer")) {
            CatConsumer consumer = new CatConsumer();
            consumer.consume();
        }
    }
}
