package com.peoplenet.kafka.embedded.fixture;

import io.confluent.examples.streams.kafka.EmbeddedSingleNodeKafkaCluster;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Collections;
import java.util.Properties;

@RunWith(SpringJUnit4ClassRunner.class)
public class EmbeddedSingleNodeKafkaClusterTest {
    private final static Logger logger = LoggerFactory.getLogger(EmbeddedSingleNodeKafkaClusterTest.class);

    private static final String SOURCE_TOPIC = "source-topic";
    private static final String SINK_TOPIC = "sink-topic";
    
    @ClassRule
    public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        CLUSTER.createTopic(SOURCE_TOPIC);
        CLUSTER.createTopic(SINK_TOPIC);
    }

    @Test
    public void testConversion() throws Exception {
        final Serde<String> stringSerde = Serdes.String();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "mid65-stream-procesor-1");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, CLUSTER.zookeeperConnect());
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        StreamsConfig config = new StreamsConfig(props);

        KStreamBuilder builder =  new KStreamBuilder();

        builder.stream(stringSerde, stringSerde, SOURCE_TOPIC).to(SINK_TOPIC);

        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();

        String puckJSON = "{\"arr\":[{\"time\":1234567999,\"qual\":1,\"heading\":45.0,\"latitude\":45.0,\"longitude\":-111.0,\"odometer\":10000.0},{\"time\":2234590000,\"qual\":0,\"heading\":32.15,\"latitude\":38.1,\"longitude\":-92.05,\"odometer\":990000.0}],\"size\":2,\"time\":1234567890,\"version\":1,\"dsn\":1}";

        Producer<String, String> testProducer = kafkaProducer();
        testProducer.send(new ProducerRecord<>(SOURCE_TOPIC, "11234567", puckJSON));
        logger.info("\n\n>>>>>>>>>> SENDING DATA TO TOPIC:  " + SOURCE_TOPIC + "\n" + puckJSON + "\n");

        Consumer<String, String> testConsumer2 = kafkaConsumer();
        testConsumer2.subscribe(Collections.singleton(SINK_TOPIC));

        ConsumerRecords<String, String> records = testConsumer2.poll(2000L);
        for (ConsumerRecord<String, String> record : records) {
            logger.info("\n\n>>>>>>>>> RECEIVED DATA FROM TOPIC:  " + SINK_TOPIC + "\n" + record.value() + "\n");
            // validate we get we what sent
            assert(record.value().equals(puckJSON));
        }

    }


    KafkaConsumer<String, String> kafkaConsumer() {
        return new KafkaConsumer<>(consumerProperties());
    }

    private Properties consumerProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "demo-group-1");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return props;
    }

    KafkaProducer<String, String> kafkaProducer() {
        return new KafkaProducer<>(producerProperties());
    }

    private Properties producerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "demo-producer-1");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

}