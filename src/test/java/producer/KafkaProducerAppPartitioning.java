package producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class KafkaProducerAppPartitioning {

    public static void main(String[] args) {
        Properties properties = createProducerProperties();

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        String topic = "my_topic";
        String key = "key1";
        String value = "Hello, Stinkyy!";

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    System.err.println("Error sending message: " + exception.getMessage());
                } else {
                    System.out.println("Message sent successfully to topic " + metadata.topic() +
                            " partition " + metadata.partition() + " offset " + metadata.offset());
                }
            }
        });

        producer.close();
    }

    private static Properties createProducerProperties() {
        Properties properties = new Properties();
        try (InputStream inputStream = new FileInputStream("/Users/navid/Documents/Dev/Kafka-Demo/src/test/java/resources/producer.properties")) {
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
            // Handle the exception if the file cannot be loaded
        }
        return properties;
    }
}


