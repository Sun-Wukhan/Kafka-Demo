package producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaProducerApp {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = createProducerProperties();

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        String topic = "my_topic";
        String key = "key1";
        String value = "Hello, Adina Bina Stinky Penis!";
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        producer.send(record);

        RecordMetadata metadata = producer.send(record).get();
        System.out.println("Message sent successfully to topic " + metadata.topic() + " partition: " + metadata.partition() + " offset: " + metadata.offset());


        producer.close();
    }

    private static Properties createProducerProperties(){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return properties;
    }
}
