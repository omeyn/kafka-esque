package tech.elephant.kafka;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simplest implementation of a Kafka msg consumer - reads the String-String pairs created by SimpleProducer.
 */
public class SimpleConsumer {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleConsumer.class);
  private static final int MAX_READ = 1000000000;
  private static final int LOG_INTERVAL = 1000000;

  public static void main(String[] args) {
    Properties props = new Properties();
    props.put("bootstrap.servers", "blackcube.home:9092");
    props.put("group.id", "test");
    props.put("enable.auto.commit", "true");
    props.put("auto.commit.interval.ms", "1000");
    props.put("session.timeout.ms", "30000");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    KafkaConsumer<String, String> consumer = new KafkaConsumer<String,String>(props);
    consumer.subscribe(Arrays.asList("test-topic-1"));

    int count = 0;
    long start = System.currentTimeMillis();
    while (count < MAX_READ) {
      ConsumerRecords<String, String> records = consumer.poll(100);
      for (ConsumerRecord<String,String> record : records) {
        count++;
        if (count % LOG_INTERVAL == 0) {
          long now = System.currentTimeMillis();
          double elapsed = (now-start)/1000d;
          LOG.info("Read so far {} in {}s for rate of {} msg/s", count, elapsed, count/elapsed);
          LOG.info("offset = {}, key = {}, value = {}", record.offset(), record.key(), record.value());
        }
      }
    }
    long end = System.currentTimeMillis();
    LOG.info("Finished reading {} in {}s", MAX_READ, (end-start)/1000f);

  }
}
