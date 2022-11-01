package com.mycompany.app;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class Producer {
    public static void main(String[] args) throws FileNotFoundException // bad programming, but not many options here
    {
        
        String bootstrapServers = "localhost:9092";        

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        
        // TODO: PICK ONE AND CREATE APPROPRIATE TOPICS IN KAFKA
        ArrayList<String> channels = new ArrayList<>();

        // TODO: set up "channels" (option 1: topic names, option 2: key names)
        // MAKE THESE NAMES MAKE SENSE; DO NOT USE THE DEFAULTS
        channels.add("ch1");
        channels.add("ch2");
        channels.add("ch3");

        // option 2: set topic name
        //String topic = "";
        

        File f = new File("smoker-temps.csv");
        Scanner fin = new Scanner(f);
        fin.nextLine(); // ignore the header
        while (fin.hasNext())
        {
            String line = fin.nextLine();
            String[] fields = line.split(",");
            // fields are: date time, smoker, channel 1, channel 2
            // index 0 always has data; indices may or may not
            String payload = "";
            String target = "";
            // check each field; while there should only be one measurement at each row can we guarantee that?
            for (int i=1; i<fields.length; i++)
            {
                if (!fields[i].isEmpty())
                {
                    payload = fields[0] + "\t" + fields[i];
                    target = channels.get(i-1);
                
                    ProducerRecord<String, String> record = null;
                    // TODO: Uncomment the appropriate option
                    // Option 1
                    // record = new ProducerRecord(target, payload);
                    // Option 2
                    // record = new ProducerRecord<>(topic, target, payload);
                    producer.send(record);
                }
            }
        }
        fin.close();
        producer.flush();
        producer.close();
    }
}
