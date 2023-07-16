package producer;

import org.apache.kafka.clients.producer.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class KafkaProducerAppErrorHandling {

    private static final int MAX_RETRIES = 3;
    private static final long RETRY_BACKOFF_MS = 1000;

    public static void main(String[] args) {
        Properties properties = createProducerProperties();

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        String topic = "my_topic";
        String key = "key1";
        String value = "Adina is my baby sister, she is so smol that when she farts a butterfly dies";

        boolean messageSent = false;
        int numRetries = 0;

        while (!messageSent && numRetries < MAX_RETRIES) {
            try {
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
                }).get(); // Blocking send with get() for simplicity, you can choose async sending

                messageSent = true; // Message sent successfully
            } catch (Exception e) {
                System.err.println("Error sending message, retrying...");
                numRetries++;
                try {
                    Thread.sleep(RETRY_BACKOFF_MS);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }
        }
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