package org.example.chapter4;


import org.example.chapter3.Cat;
import org.example.chapter3.CatProducer;

import java.util.ArrayList;

public class App {
    public static void main(String[] args) {
        String kind = args[0];
        if (kind.equals("producer")) {
            ArrayList<Cat> cats = new ArrayList<>();
            CatProducer p = new CatProducer("localhost:9092", "angrycat");

            for (int i = 0; i < 10; i++) {
                String name = "大橘" + i + "号";
                Cat c = Cat.builder().name(name).kind("angry").build();
                cats.add(c);
            }
            p.send(cats);
        } else if (kind.equals("range_consumer")) {
            CuteCatRangerConsumer.consume();
        }

    }
}
