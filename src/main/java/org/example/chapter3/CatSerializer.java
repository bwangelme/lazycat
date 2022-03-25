package org.example.chapter3;

import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class CatSerializer implements Serializer<Cat> {
    public CatSerializer() {

    }

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String topic, Cat cat) {
        if (cat == null) {
            return null;
        }
        byte[] name, kind;
        if (cat.getName() != null) {
            name = cat.getName().getBytes(StandardCharsets.UTF_8);
        } else {
            name = new byte[0];
        }

        if (cat.getKind() != null) {
            kind = cat.getKind().getBytes(StandardCharsets.UTF_8);
        } else {
            kind = new byte[0];
        }

        ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + name.length + kind.length);
        buffer.putInt(name.length);
        buffer.put(name);
        buffer.putInt(kind.length);
        buffer.put(kind);

        return buffer.array();
    }

    @Override
    public void close() {

    }
}
