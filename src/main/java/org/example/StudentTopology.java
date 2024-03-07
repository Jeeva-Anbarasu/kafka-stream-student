package org.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.example.model.StudentDetails;
import org.example.serdes.SerdesFactory;

@Slf4j
public class StudentTopology {

    public static String SECTION_A = "section_A";

    public static String SECTION_B = "section_B";

    public  static Predicate<String, StudentDetails> section_A = (Key , StudentDetails) ->
        StudentDetails.getDetails().section.equals("A");

    public  static Predicate<String, StudentDetails> section_B = (Key , StudentDetails) ->
            StudentDetails.getDetails().section.equals("B");

    public static Topology creatStudentTopology(){

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String , StudentDetails> original  = streamsBuilder.stream(MockData.MOCK_DATA , Consumed.with(Serdes.String() , SerdesFactory.studentSerdes()));

        BranchedKStream<String , StudentDetails> modifiedStream = original.split()
                .branch(section_A , Branched.withConsumer(sectionAStream ->
                {
                    sectionAStream.to(SECTION_A , Produced.with(Serdes.String() , SerdesFactory.studentSerdes()));
                }));

        return streamsBuilder.build();
    }
}
