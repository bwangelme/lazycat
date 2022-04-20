package org.example.chapter3;

public class App {
    public static void main(String[] args) {
        String kind = args[0];
        switch (kind) {
            case "producer": {
                CatProducer p = new CatProducer("localhost:9092", "angrycat");
                Integer partition = 0;
                String name = "xiaobai " + partition.toString();
                Cat c = Cat.builder().name(name).kind("cute").build();
                p.send(c, partition);
            }
            case "consumer":
                Consumer consumer = new Consumer();
                consumer.consume();
                break;
            case "seek_consumer": {
                SeekConsumer c = new SeekConsumer();
                c.consume();
                break;
            }
        }
    }
}
