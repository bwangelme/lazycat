package org.example.zigzag;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class App {
    private static final byte[] HEX_ARRAY = "0123456789ABCDEF".getBytes(StandardCharsets.UTF_8);

    public static void main(String[] args) {
        ByteBuffer buffer = ByteBuffer.allocate(10);

        App a = new App();
        a.writeVariant(-23, buffer);
        System.out.println("variant bytes is `" + bytesToHex(buffer.array()) + "`");

        int b = a.readVariant(buffer);
        System.out.println("decode value is " + b);
    }

    public int readVariant(ByteBuffer buffer) throws RuntimeException {
        int value = 0;
        int i = 0;
        int b;

        buffer.rewind();
        // 从字节流中取出一个字节，如果第8位是0的话，表示读取整数结束
        while (((b = buffer.get()) & 0x80) != 0) {
            // 取出低7位，并左移 7 位放到 value 中
            b = (b & 0x7f) << i;
            System.out.println(b);
            value |= b;
            i += 7;
            // 32 位整数最大编码结果是5个字节，在循环中最多只能读4次
            if (i > 28) {
                throw new RuntimeException("illegal bytes");
            }
        }

        // 最后一次在循环外读取，且由于最后一位最高位是0,也不需要进行去除最高位的操作
        value |= b << i;
        return (value >>> 1) ^ -(value & 1);
    }

    public void writeVariant(int value, ByteBuffer buffer) {

        // 将整数 value 进行 zigzag 编码, int 是 32 位，所以这里直接写死了 31
        int v = (value << 1) ^ (value >> 31);

        // 判断 v 的低7位是否是0
        while ((v & 0xffffff80) != 0L) {
            // 取出 v 的低7位，并在最高位上加上1
            byte b = (byte) ((v & 0x7f) | 0x80);
            // 放到 buffer 中
            // 因为是先放整数的低字节位，所以这里实现了小端字节序
            buffer.put(b);
            // 将 v 无符号右移 7 位，最高位补 0
            v >>>= 7;
        }

        // 最后取出的 7 位最高位一定是0,且它位于 variant 编码的最后一个字节，需要设置成0
        buffer.put((byte) v);
    }

    public static String bytesToHex(byte[] bytes) {
        byte[] hexChars = new byte[bytes.length * 3];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 3] = HEX_ARRAY[v >>> 4];
            hexChars[j * 3 + 1] = HEX_ARRAY[v & 0x0F];
            hexChars[j * 3 + 2] = ' ';
        }
        return new String(hexChars, StandardCharsets.UTF_8);
    }

}
