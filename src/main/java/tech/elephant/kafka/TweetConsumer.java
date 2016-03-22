package tech.elephant.kafka;

import java.util.Arrays;
import java.util.Properties;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A kafka consumer for the avro-encoded tweet stream produced by the TweetProducer. Uses the Confluent Schema Registry.
 */
public class TweetConsumer {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleConsumer.class);
  private static final int MAX_READ = 1000000000;
  private static final int LOG_INTERVAL = 100;

  public static void main(String[] args) {
    // setup kafka
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "blackcube.home:9092");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
    props.put("schema.registry.url", "http://confluent.home:8081");

    LOG.info("Starting TweetConsumer");
    KafkaConsumer<Long, GenericRecord> consumer = new KafkaConsumer<Long, GenericRecord>(props);
    consumer.subscribe(Arrays.asList("tweets"));

    int count = 0;
    long start = System.currentTimeMillis();
    while (count < MAX_READ) {
      ConsumerRecords<Long, GenericRecord> records = consumer.poll(100);
      for (ConsumerRecord<Long, GenericRecord> record : records) {
        count++;
        if (count % LOG_INTERVAL == 0) {
          long now = System.currentTimeMillis();
          double elapsed = (now - start) / 1000;
          LOG.info("Read so far {} in {}s for rate of {} msg/s", count, elapsed, Math.round(count / elapsed));
          LOG.info("offset = {}, key = {}, value = {}", record.offset(), record.key(), record.value());
        }
      }
    }
    long end = System.currentTimeMillis();
    LOG.info("Finished reading {} in {}s", MAX_READ, (end - start) / 1000f);
  }
}
