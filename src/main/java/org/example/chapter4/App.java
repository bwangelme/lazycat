package org.example.chapter4;

import org.example.chapter3.Cat;
import org.example.chapter3.CatProducer;

public class App {
    public static void main(String[] args) {
        String kind = args[0];
        if (kind.equals("producer")) {
            CatProducer p = new CatProducer("localhost:9092", "angrycat");

            for (int i = 0; i < 10; i++) {
                Integer partition = i % 5;
                String name = "大橘" + i + "号";
                Cat c = Cat.builder().name(name).kind("angry").build();
                p.send(c, partition);
            }
        } else if (kind.equals("range_consumer")) {
            CuteCatRangerConsumer.consume();
        }

    }
}
