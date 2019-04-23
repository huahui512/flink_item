package examples;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.UUID;

/**
 * @author wangzhihua
 * @date 2019-04-18 14:32
 */
public class KafkaSinkUtil extends RichSinkFunction<String> {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSinkUtil.class);
    private String servers;
    private String topic;
    private KafkaProducer<String, String> producer;
    public KafkaSinkUtil() {
    }

    public KafkaSinkUtil(String servers, String topic) {
        this.servers = servers;
        this.topic = topic;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", this.servers);
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("compression.type", "lz4");
        producer = new KafkaProducer<>(props);
        super.open(parameters);
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        producer.send(new ProducerRecord<>(this.topic, UUID.randomUUID().toString(), value));

    }

    @Override
    public void close() throws Exception {
        producer.close();
        super.close();
    }

}
