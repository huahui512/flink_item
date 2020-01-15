package com.huahui.test;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author wangtaiyang
 */
public class ConsumerKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test3");
        properties.setProperty("auto.offset.reset" , "earliest");
        FlinkKafkaConsumer<String> test = new FlinkKafkaConsumer<>("DimensionTablejoin", new SimpleStringSchema(), properties);
        DataStreamSource<String> stringDataStreamSource = env.addSource(test);

        CheckpointConfig cpConfig = env.getCheckpointConfig();
        env.enableCheckpointing(5000);
        cpConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
        cpConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        StateBackend rocksDBStateBackend = new RocksDBStateBackend("hdfs:///checkpoints-data/");
        env.setStateBackend(rocksDBStateBackend);
        stringDataStreamSource.map(value -> {
            System.out.println("2222222222");
            return "#" + value + "#2222222";
        }).uid("a").print();
        /*stringDataStreamSource.map(value -> {
            return "@" + value + "@";
        }).uid("b").print();
        stringDataStreamSource.map(value -> {
            return "@" + value + "@";
        }).uid("c").print();
        stringDataStreamSource.print();*/
        env.execute("aaa");

    }
}
