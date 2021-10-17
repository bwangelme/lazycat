package org.example.chapter2;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class CompanyDeserializer implements Deserializer<Company> {
    public void configure(Map<String, ?> var1, boolean var2) {

    }

    public Company deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        if (data.length < 8) {
            throw new SerializationException("Size of data received by CompanyDeserializer is shorter than expected!");
        }

        ByteBuffer buffer = ByteBuffer.wrap(data);
        int nameLen, addressLen;

        nameLen = buffer.getInt();
        byte[] nameBytes = new byte[nameLen];
        buffer.get(nameBytes);
        addressLen = buffer.getInt();
        byte[] addressBytes = new byte[addressLen];
        buffer.get(addressBytes);

        String name, address;
        name = new String(nameBytes, StandardCharsets.UTF_8);
        address = new String(addressBytes, StandardCharsets.UTF_8);

        return Company.builder().name(name).address(address).build();
    }

    public void close() {

    }
}
