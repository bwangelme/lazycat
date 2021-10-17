package org.example.chapter1;

/**
 * Kafka Example
 */
public class App {
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
