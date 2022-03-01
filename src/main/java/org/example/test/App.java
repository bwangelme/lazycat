package org.example.test;

public class App {
    public static void main(String[] args) {
        App a = new App();
        a.showZigZag();
    }

    void showZigZag() {
        System.out.print("原值|编码后的值\n---|---\n");
        for (int i = -20; i < 20; i++) {
            System.out.printf("%d|%d\n" ,i, this.getZigZag(i));
        }
    }

    void shift() {
        // >> 是右移运算符，最高位根据符号位补位
        // >>> 是无符号右移运算符，忽略符号位，最高位补0
        System.out.println(23 >> 1);
        System.out.println(-4 >> 1);
        System.out.println(-4 >>> 1);
    }

    int getZigZag(int n) {
        return (n << 1) ^ (n >> 31);
    }
}
