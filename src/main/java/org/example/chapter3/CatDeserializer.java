package org.example.chapter3;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class CatDeserializer implements Deserializer<Cat> {
    public void configure(Map<String, ?> var1, boolean var2) {
    }

    public Cat deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        if (data.length < 8) {
            throw new SerializationException("Size of data received by CompanyDeserializer is shorter than expected!");
        }

        ByteBuffer buffer = ByteBuffer.wrap(data);
        int nameLen, kindLen;

        nameLen = buffer.getInt();
        byte[] nameBytes = new byte[nameLen];
        buffer.get(nameBytes);
        kindLen = buffer.getInt();
        byte[] kindBytes = new byte[kindLen];
        buffer.get(kindBytes);

        String name, kind;
        name = new String(nameBytes, StandardCharsets.UTF_8);
        kind = new String(kindBytes, StandardCharsets.UTF_8);

        return Cat.builder().name(name).kind(kind).build();
    }

    public void close() {

    }
}
