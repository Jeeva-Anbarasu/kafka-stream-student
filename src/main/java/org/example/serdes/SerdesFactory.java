package org.example.serdes;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.example.model.StudentDetails;

public class SerdesFactory {

    public static Serde<StudentDetails> studentSerdes(){

        JsonSerialization<StudentDetails> jsonSerialization = new JsonSerialization<>();

        JsonDeserialization<StudentDetails> jsonDeserialization = new JsonDeserialization<>(StudentDetails.class);

        return  Serdes.serdeFrom(jsonSerialization , jsonDeserialization);
    }
}
