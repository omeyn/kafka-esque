package tech.elephant.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleProducer {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleProducer.class);
  private static final int MAX_WRITE = 100000000;

  public static void main(String[] args) {
    Properties props = new Properties();
    props.put("bootstrap.servers", "blackcube.home:9092");
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 33554432);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    LOG.info("Starting SimpleProducer");
    Producer<String, String> producer = new KafkaProducer<String, String>(props);
    long start = System.currentTimeMillis();
    for (int i = 0; i < MAX_WRITE; i++) {
      producer.send(new ProducerRecord<String, String>("test-topic-1", Integer.toString(i), Integer.toString(i)));
    }
    producer.close();
    LOG.info("Closing SimpleProducer");
    long end = System.currentTimeMillis();
    double totalTime = (end - start)/1000d;
    LOG.info("Finished writing {} in {}s = {} msg/s", MAX_WRITE, totalTime, MAX_WRITE / totalTime);
  }
}
