package com.github.fnb.assessment.katlego;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class StreamEventApp {



    public static void main(String[] args) throws IOException {
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-test-app";
        String auto_offset_reset = "earliest";
        String topic = "input-topic";
        String outTopic = "output-topic";
        Logger logs = LoggerFactory.getLogger(StreamEventApp.class.getName());
        final CountDownLatch latch = new CountDownLatch(1);

        String filePath = "src/test.json";
        String line;
        String key ="";
        String value="";


        //FIELD STRUCTURE FOR CANDIDATE DETAILS
        String[][] arrayDetails = {{"candidateName", "Katlego"}, {"candidateLastName", "Mohlala"},{"candidateContactNumber", "0813077848"}};

        //Stream.of(arrayDetails);
        /////////////
        // create consumer configs
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-event-app");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-event-app");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, auto_offset_reset);

        /*
        Map<String, String> arrMap = Arrays.stream(arrayDetails).collect(Collectors.toMap(e -> e[0], e -> e[1]));
        ObjectMapper mapper = new ObjectMapper();
       // String jsonResult = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(arrMap);
       // System.out.println(jsonResult);

        BufferedReader reader = new BufferedReader(new FileReader(filePath));
        HashMap<String, String> map = new LinkedHashMap<String, String>();
        while ((line = reader.readLine()) != null) {
            String[] tokens = line.split(":", 2);
            if (tokens.length >= 2) {
                key = tokens[0];
                value = tokens[1];
                map.put(key, value);


            } else {
                //System.out.println("leave out curly braces: " + line);
            }

        }
        map.putAll(arrMap);
        String jsonResult = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(map);

        System.out.println(jsonResult);
        JsonNode masterJSON = mapper.readTree(jsonResult);
        */


        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> streamtopic = builder.stream(topic);
        streamtopic.flatMapValues(person -> Arrays.asList(Arrays.stream(arrayDetails))).to(outTopic);


       KafkaStreams streams = new KafkaStreams(new Topology(), properties);
       streams.start();




    }

}
