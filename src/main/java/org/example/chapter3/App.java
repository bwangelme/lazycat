package org.example.chapter3;

public class App {
    public static void main(String[] args) {
        String kind = args[0];
        if (kind.equals("producer")) {
            Producer p = new Producer();
            Integer partition = 1;
            String name = "xiaohei " + partition.toString();
            Cat c = Cat.builder().name(name).kind("cute").build();
            p.send(c, partition);
        } else if (kind.equals("consumer")) {
            Consumer consumer = new Consumer();
            consumer.consume();
        }

    }
}
