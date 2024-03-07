package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.model.ClassDetails;
import org.example.model.StudentDetails;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Slf4j
public class MockData {


    public static KafkaProducer<String, String> producer = new KafkaProducer<String , String>(producerProperties());

    public static final String MOCK_DATA = "mock_data";
    public static Properties producerProperties(){

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG , "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG , StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG , StringSerializer.class.getName());
        return properties;

    }


    private static List<StudentDetails> createStudents(){
        StudentDetails ram = new StudentDetails( 1 ,"Ram" ," Kumar" , new ClassDetails( "B") , "Madurai");
        StudentDetails jeeva = new StudentDetails( 2 ,"Jeeva" ,"Anbarasu" , new ClassDetails( "B") , "Coimbatore");
        StudentDetails hari = new StudentDetails( 3,"Hari" ,"Prasanth" , new ClassDetails( "A") , "Chennai");

        return List.of(ram , jeeva ,hari);
    }

    public static void main(String[] args) {

        List<StudentDetails> studentDetails = createStudents();

        ObjectMapper objectMapper = new ObjectMapper();
        studentDetails.stream()
                .map( student -> {
                    try {
                        String stu = objectMapper.writeValueAsString(student);
                        ProducerRecord<String , String> record = new ProducerRecord<>(MOCK_DATA , student.id + " " , stu);
                        producer.send(record).get();

                    } catch (JsonProcessingException | ExecutionException | InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                            return student;
                }
                ).collect(Collectors.toList());

    }
}
