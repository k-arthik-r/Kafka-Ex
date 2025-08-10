package com.voidex.kafka;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import static com.voidex.kafka.KafkaConstants.BOOTSTRAP_SERVER;
import static com.voidex.kafka.KafkaConstants.NUMBER_OF_PARTITIONS;
import static com.voidex.kafka.KafkaConstants.SNAPCHAT;
import static com.voidex.kafka.KafkaConstants.WHATSAPP;


public class KafkaProducerSample {
    public static <K> void main(String[] args) throws Exception{

        try(AdminClient adminClient = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER));) {
            
            NewTopic whatsapp = new NewTopic(WHATSAPP, NUMBER_OF_PARTITIONS.intValue(), (short) 1);
            NewTopic snapchat = new NewTopic(SNAPCHAT, NUMBER_OF_PARTITIONS.intValue(), (short) 1);
            
            adminClient.createTopics(Arrays.asList(whatsapp, snapchat)).all().get();
            System.out.println("Topics created: whatsapp, snapchat");

        }

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter messages in format: <topic> <partition> <message>");
        System.out.println("Type 'exit' to quit.");

        try {
            while (true) {
                System.out.print("> ");
                String line = scanner.nextLine();
                if (line.equalsIgnoreCase("exit")) break;

                String[] parts = line.split(" ", 3);
                if (parts.length < 3) {
                    System.out.println("Invalid format! Example: whatsapp 0 Hello");
                    continue;
                }

                String topic = parts[0];
                int partition = Integer.parseInt(parts[1]);
                String message = parts[2];

                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, partition, "user_" + (partition + 1), message);
                kafkaProducer.send(producerRecord);
                System.out.println("Sent to " + topic + " partition " + partition);
            }
        } finally{
            kafkaProducer.close();
            scanner.close();
            try(AdminClient adminClient = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER))){
                adminClient.deleteTopics(Arrays.asList(WHATSAPP, SNAPCHAT)).all().get();
                System.out.println("Topics deleted: whatsapp, snapchat");
            }
        }
    }
}