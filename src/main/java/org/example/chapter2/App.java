package org.example.chapter2;

public class App {
    public static void main(String[] args) {
        String kind = args[0];
        if (kind.equals("producer")) {
            Integer partition = 1;
            String name = "douban " + partition.toString();
            Company c = Company.builder().name(name).address("jiuxianqiao").build();
            CatProducer.send(c, partition);
        } else if (kind.equals("consumer")) {
            CatConsumer.consume();
        }
    }
}
