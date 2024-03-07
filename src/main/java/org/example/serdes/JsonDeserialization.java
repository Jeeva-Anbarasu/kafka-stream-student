package org.example.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;

public class JsonDeserialization<T> implements Deserializer<T> {

    public Class<T> destination;
    public static ObjectMapper objectMapper = new ObjectMapper();


    public JsonDeserialization(Class<T> destination){
        this.destination = destination;
    }
    @Override
    public T deserialize(String s, byte[] bytes) {
        try {
            return objectMapper.readValue(bytes , destination);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
