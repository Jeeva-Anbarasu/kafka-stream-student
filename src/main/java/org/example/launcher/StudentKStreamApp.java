package org.example.launcher;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.example.MockData;
import org.example.StudentTopology;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class StudentKStreamApp {

    public static Properties createProperties(){

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG , "orders-1");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG , "localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG , "latest");
        return properties;
    }

    public static void main(String[] args) {

        AdminClient adminClient = AdminClient.create(createProperties());

        Topology topology = StudentTopology.creatStudentTopology();

        List<String> topics = List.of(MockData.MOCK_DATA , StudentTopology.SECTION_A ,StudentTopology.SECTION_B);

        List<NewTopic> listTopics = topics.stream().map( topic -> {
                            NewTopic newTopic = new NewTopic(topic , 1 , (short) 1);
                            return newTopic;
                        }
                        ).collect(Collectors.toList());

        adminClient.createTopics(listTopics);

        KafkaStreams kafkaStreams = new KafkaStreams(topology , createProperties());

        kafkaStreams.start();
    }
}
