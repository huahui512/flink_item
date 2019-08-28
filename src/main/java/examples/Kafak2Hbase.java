package examples;


import com.missfresh.cn.CountAgg;
import com.missfresh.cn.ItemViewCount;
import com.missfresh.cn.WindowResult;
import com.missfresh.util.HbaseUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;


import java.util.ArrayList;
import java.util.Properties;

/**
 * @author wangzhihua
 * @date 2018-12-26 14:32
 */
public class Kafak2Hbase {
    public static void main(String[] args) throws Exception {
        String kafkaBrokers="10.2.40.10:9092,10.2.40.15:9092,10.2.40.14:9092";
        String topic ="flink_log3";
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
        FlinkKafkaConsumer010<String> kafkaConsumer010 = new FlinkKafkaConsumer010<>(topic, new SimpleStringSchema(), properties);
        kafkaConsumer010.setStartFromTimestamp(1562208050000l);
        DataStreamSource<String> kafkaData = env.addSource(kafkaConsumer010);
        kafkaData.print();
/*
        SingleOutputStreamOperator<Tuple5<Long, Long, Long, String, Long>> data1 = kafkaData.map(new MapFunction<String, Tuple5<Long, Long, Long, String, Long>>() {
            @Override
            public Tuple5<Long, Long, Long, String, Long> map(String s) throws Exception {
                Tuple5<Long, Long, Long, String, Long> userInfo=null;
                if(!s.equals("")){
                String[] split = s.split(",");
                    long userID = Long.parseLong(split[0]);
                    long itemId = Long.parseLong(split[1]);
                    long categoryId = Long.parseLong(split[2]);
                    String behavior = split[3];
                    long timestamp = Long.parseLong(split[4]);
                    userInfo = new Tuple5<>(userID, itemId, categoryId, behavior, timestamp);

                }else {
                    System.out.println(s);
                }

                return userInfo;
            }
        });
        //写入hbase
        data1.writeUsingOutputFormat(new HBaseOutputFormat());

        System.out.println("===============》 flink任务结束  ==============》");*/

        //设置程序名称
        env.execute("data_from_kafak_wangzh");
       /* HbaseUtil.close();*/

    }
}
