package org.example.chapter3;

public class App {
    public static void main(String[] args) {
        String kind = args[0];
        switch (kind) {
            case "producer": {
                Producer p = new Producer();
                Integer partition = 0;
                String name = "xiaobai " + partition;
                Cat c = Cat.builder().name(name).kind("cute").build();
                p.send(c, partition);
                break;
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
