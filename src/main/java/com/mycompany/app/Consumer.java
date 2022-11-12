/**
 * Class: 44-517 Big Data
 * Author: Jacob Kapfer
 * Description: kafka-multichannel
 * Due: 11/11/22
 * I pledge that I have completed the programming assignment independently.
   I have not copied the code from a student or any source.
   I have not given my code to any other student.
   I have not given my code to any other student and will not share this code
   with anyone under any circumstances.
*/
package com.mycompany.app;

import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Queue;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Consumer {

    public static void main(String[] args)
    {
        String bootstrapServers = "localhost:9092"; 

        Properties properties= new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "oop");


        KafkaConsumer<String, String> smokerConsumer = new KafkaConsumer<>(properties);
        smokerConsumer.subscribe(Arrays.asList("smoker"));

        KafkaConsumer<String, String> food1Consumer = new KafkaConsumer<>(properties);
        food1Consumer.subscribe(Arrays.asList("food1"));
        
        KafkaConsumer<String, String> food2Consumer = new KafkaConsumer<>(properties);
        food2Consumer.subscribe(Arrays.asList("food2"));

        Queue<Double> temps = new LinkedList<>();
        Queue<String> times = new LinkedList<>();

        Queue<Double> temps2 = new LinkedList<>();
        Queue<String> times2 = new LinkedList<>();

        Queue<Double> temps3 = new LinkedList<>();
        Queue<String> times3 = new LinkedList<>();


        Boolean stall1 = false;
        Boolean stall2 = false;
        
        
        while(true) {
            ConsumerRecords<String, String> smokerRecords = smokerConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record: smokerRecords)
            {
                //System.out.println(record.topic() + ", " + record.key() + ", " + record.value());
                // Queue<Double> temps = new LinkedList<>();
                // Queue<String> times = new LinkedList<>();
                String[] values = record.value().split("\t");
                //adding values to queues
                times2.add(values[0]);
                temps2.add(Double.parseDouble(values[1]));
                // System.out.println(temps.peek());
                //20 records is 10 min
                
                if(temps2.size()>5)
                {
                    if(Double.parseDouble(values[1]) < temps2.peek() - 15)
                    {
                        System.out.println(values[0] + " Check Smoker!!!");
                    }
                    temps2.remove();
                    times2.remove();
                }
            }
            ConsumerRecords<String, String> food1Records = food1Consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record: food1Records)
            {
                //System.out.println(record.topic() + ", " + record.key() + ", " + record.value());
                
                String[] values = record.value().split("\t");
                //adding values to queues
                times.add(values[0]);
                temps.add(Double.parseDouble(values[1]));
                //20 records is 10 min
                
                if(stall1 == false && temps.size()>=20)
                {
                    if((Double.parseDouble(values[1]) <= temps.peek()+1))
                    {
                        System.out.print("Food1 has hit stall at : ");
                        System.out.println(values[0]);    
                        stall1 = true;
                    }

                    temps.remove();
                    times.remove();
                }
            }
            ConsumerRecords<String, String> food2Records = food2Consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record: food2Records)
            {
                //System.out.println(record.topic() + ", " + record.key() + ", " + record.value());
                
                String[] values = record.value().split("\t");
                //adding values to queues
                times3.add(values[0]);
                temps3.add(Double.parseDouble(values[1]));
                //20 records is 10 min
                
                if(stall2 == false && temps3.size()>=20)
                {
                    if((Double.parseDouble(values[1]) <= temps3.peek()+1))
                    {
                        System.out.print("Food2 has hit stall at : ");
                        System.out.println(values[0]);    
                        stall2 = true;
                    }

                    temps3.remove();
                    times3.remove();
                }
            }
        }
        


    }
    
}
