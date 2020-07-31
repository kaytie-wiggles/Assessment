package com.github.fnb.assessment.katlego;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.*;
import java.util.*;

public class producerInput {
    public static void main( String[] args ) throws IOException
    {
        String filePath = "src/test.json";
        String bootstrapServers = "127.0.0.1:9092";
        String topic = "input-topic";
        String line;
        String key ="";
        String value="";

        //Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        BufferedReader reader = new BufferedReader(new FileReader(filePath));
        HashMap<String, String> map = new LinkedHashMap<String, String>();

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        ObjectMapper mapper = new ObjectMapper();

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
//producer record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
        producer.send(record);
        String jsonRes = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(map);
        //System.out.println(jsonRes);

        try (FileWriter file = new FileWriter("datain.json")) {

            file.write(jsonRes);
            file.flush();

        } catch (IOException e) {
            e.printStackTrace();
        }
        //System.out.println(list);
        reader.close();


        producer.flush();
        producer.close();

    }
}
