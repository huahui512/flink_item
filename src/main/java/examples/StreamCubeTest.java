package examples;

import kafka.producer.KeyedMessage;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.UUID;

/**
 * @author wangzhihua
 * @date 2019-03-14 15:57
 */
public class StreamCubeTest {
    private final Producer<String, String> producer;
    public final static String TOPIC = "rtsc_stream_test_ods";
    public final static String MESSAGE = "{\"current_timestamp\":\"1560321490665\",\"remote_ip\":\"10.13.16.3:60188\",\"user_id\":\"133871674\",\"business\":\"mryt\",\"platform\":\"wxapp\",\"version\":\"4.7.3\",\"device_id\":\"none\",\"device_source_id\":\"none\",\"device_model\":\"FIG-AL10\",\"device_os\":\"Android 8.0.0\",\"device_size\":\"0*0\",\"address_code\":\"0\",\"station_code\":\"-\",\"anchor_id\":\"4.7.31560309767696\",\"from_source\":\"-\",\"action\":\"-\",\"sku\":\"-\",\"module_standby\":\"-\",\"promotion_id\":\"-\",\"channel\":\"-\",\"second_channel\":\"-\",\"type\":\"-\"}";
    private StreamCubeTest(){
        Properties props = new Properties();
        props.setProperty("bootstrap.servers","10.2.40.14:9092,10.2.40.10:9092,10.2.40.15:9092");
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("compression.type", "lz4");
        producer = new KafkaProducer<>(props);
    }

    void produce() throws InterruptedException {
        while (true) {
            producer.send(new ProducerRecord<>(TOPIC, UUID.randomUUID().toString(), MESSAGE));
            Thread.sleep(500);
        }
    }

    public static void main( String[] args ) throws InterruptedException {
        new StreamCubeTest().produce();
    }
}
