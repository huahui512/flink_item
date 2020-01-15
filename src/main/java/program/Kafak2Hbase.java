package program;


import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;


import java.util.Properties;

/**
 * @author wangzhihua
 * @date 2018-12-26 14:32
 */
public class Kafak2Hbase {
    public static void main(String[] args) throws Exception {
        String kafkaBrokers="127.0.0.1:9092";
        String topic ="test";
        String groupId="uu";

        System.out.println("===============》 flink任务开始  ==============》");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置kafka连接参数
        Properties  properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaBrokers);
        properties.setProperty("group.id", groupId);
        //设置时间类型
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置检查点时间间隔 默认30秒
        env.enableCheckpointing(5000);
        //设置检查点模式 默认就是EXACTLY_ONCE
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        System.out.println("===============》 开始读取kafka中的数据  ==============》");
        //创建kafak消费者，获取kafak中的数据
        FlinkKafkaConsumer<String> kafkaConsumer010 = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        kafkaConsumer010.setStartFromTimestamp(1562208050000l);
        DataStreamSource<String> kafkaData = env.addSource(kafkaConsumer010);
        kafkaData.print();
        env.execute("data_from_kafak_wangzh");

    }
}
