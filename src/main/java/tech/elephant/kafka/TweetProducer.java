package tech.elephant.kafka;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;

/**
 * Reads the Twitter API "gardenhose" (1% firehose sample) and writes those tweets into Kafka as Avro messages, using
 * the Confluent Schema Registry so that Connectors can read the messages.
 */
public class TweetProducer {

  private static final Logger LOG = LoggerFactory.getLogger(TweetProducer.class);

  private static final String API_KEY = "apikey";
  private static final String SECRET_KEY = "apisecret";
  private static final String ACCESS_TOKEN_KEY = "accesstoken";
  private static final String ACCESS_SECRET_KEY = "accesstokensecret";

  private static final String TOPIC = "tweets";

  public static void main(String[] args) {
    // setup kafka
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "blackcube.home:9092");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
    props.put("schema.registry.url", "http://confluent.home:8081");
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.RETRIES_CONFIG, 0);
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
    props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);

    LOG.info("Starting TweetProducer");
    KafkaProducer producer = new KafkaProducer(props);

    // setup twitter and start the stream
    Properties authProps = new Properties();
    try (InputStream inputStream = TweetProducer.class.getClassLoader().getResourceAsStream("twitter-auth.properties")) {
      authProps.load(inputStream);
    } catch (IOException e) {
      LOG.error("Couldn't open auth props file", e);
      System.exit(1);
    }
    TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
    twitterStream.setOAuthConsumer(authProps.getProperty(API_KEY), authProps.getProperty(SECRET_KEY));
    twitterStream.setOAuthAccessToken(new AccessToken(authProps.getProperty(ACCESS_TOKEN_KEY), authProps.getProperty(ACCESS_SECRET_KEY)));
    StatusListener listener = new KafkaPublishingStatusListener(producer, TOPIC);
    twitterStream.addListener(listener);
    twitterStream.sample("en");

    // TODO shut down hook
//    producer.close();
//    LOG.info("Closing TweetProducer");

  }

  private static class KafkaPublishingStatusListener implements StatusListener {

    private static final int REPORT_COUNT = 100;

    private static final String USER_SCHEMA = "{\"type\":\"record\"," +
                        "\"name\":\"tweet\"," +
                        "\"fields\":[{\"name\":\"id\",\"type\":\"long\"},"
                        + "{\"name\":\"username\",\"type\":\"string\"},"
                        + "{\"name\":\"status\",\"type\":\"string\"}]}";
    private static final Schema.Parser PARSER = new Schema.Parser();
    private static final Schema SCHEMA = PARSER.parse(USER_SCHEMA);

    private final KafkaProducer producer;
    private final String topic;
    private final long start;

    private long tweetCount = 0;


    public KafkaPublishingStatusListener(KafkaProducer producer, String topic) {
      this.producer = producer;
      this.topic = topic;
      start = System.currentTimeMillis();
    }

    public void onStatus(Status status) {
      tweetCount++;
      if (tweetCount % REPORT_COUNT == 0) {
        long now = System.currentTimeMillis();
        LOG.info("Published {} tweets to kafka since start (at {} msg/s)", tweetCount, tweetCount / ((now-start)/1000));
      }

      GenericRecord avroRecord = new GenericData.Record(SCHEMA);
      avroRecord.put("id", status.getId());
      avroRecord.put("username", status.getUser().getName());
      avroRecord.put("status", status.getText());

      producer.send(new ProducerRecord<Object, Object>(topic, status.getId(), avroRecord));
    }

    public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
//      LOG.info("Deletion: {}", statusDeletionNotice.getUserId());
    }

    public void onTrackLimitationNotice(int i) {
      LOG.debug("TrackLimitationNotice: {}", i);
    }

    public void onScrubGeo(long l, long l1) {
      LOG.debug("scrub geo: {} {}", l, l1);
    }

    public void onStallWarning(StallWarning stallWarning) {
      LOG.warn("Stall warning: {}", stallWarning.getMessage());
    }

    public void onException(Exception e) {
      LOG.warn("Bubbled exception", e);
    }
  }
}
