package org.example.chapter2;

public class App {
    public static void main(String[] args) {
        String kind = args[0];
        if (kind.equals("producer")) {
            CatProducer p = new CatProducer();
            Company c = Company.builder().name("douban").address("jiuxianqiao").build();
            p.send(c);
        } else if (kind.equals("consumer")) {
            CatConsumer consumer = new CatConsumer();
            consumer.consume();
        }
    }
}
