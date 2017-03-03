package io.taskforcex.tool;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by dgt on 3/1/17.
 */
public class KafkaPublisher implements IPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaPublisher.class);

    private final Producer<String, String> producer;

    public KafkaPublisher(String kafkaServers) {
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        /*
         * The acks config controls the criteria under which requests are considered complete.
         * The "all" setting we have specified will result in blocking on the full commit of the record,
          * the slowest but most durable setting.
         */
        kafkaProps.put(ProducerConfig.ACKS_CONFIG, "all");

        /* If the request fails, the producer can automatically retry, though since we have specified retries as 0 it won't.
         * Enabling retries also opens up the possibility of duplicates.
         */
        kafkaProps.put(ProducerConfig.RETRIES_CONFIG, 0);

        /* By default a buffer is available to send immediately even if there is additional unused space in the buffer.
         * However if you want to reduce the number of requests you can set linger.ms to something greater than 0.
         * This will instruct the producer to wait up to that number of milliseconds before sending a request in hope
         * that more records will arrive to fill up the same batch.
         *
         * This is analogous to Nagle's algorithm in TCP.
         * Note that records that arrive close together in time will generally batch together even with linger.ms=0 so
         * under heavy load batching will occur regardless of the linger configuration; however setting this to something
         * larger than 0 can lead to fewer, more efficient requests when not under maximal load at the cost of a small amount of latency.
         */
        kafkaProps.put(ProducerConfig.LINGER_MS_CONFIG, 1000);

        LOG.info("Setup Kafka pub: " + kafkaProps);
        producer = new KafkaProducer<>(kafkaProps);
    }

    public Future<?> pubAsync(String topic, String key, String msg) {
        LOG.debug(String.format("Publish succeed. topic=%s, key=%s, msg=%s", topic, key, msg));
        return producer.send(new ProducerRecord<>(topic, key, msg));
    }

    public void pubSync(String topic, String key, String msg) {
        try {
            producer.send(new ProducerRecord<>(topic, key, msg)).get(3, TimeUnit.SECONDS);
            LOG.debug(String.format("Publish succeed. topic=%s, key=%s, msg=%s", topic, key, msg));
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            LOG.error(String.format("Publish failed. topic=%s, key= %s, msg=%s", topic, key, msg));
            throw new RuntimeException("Publish fail", e);
        }
    }

    public void close() {
        producer.close();
    }
}
