package com.missfresh.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class GrampusOrderETL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test9");
//        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("auto.offset.reset", "latest");
        FlinkKafkaConsumer010<String> test = new FlinkKafkaConsumer010<>("grampus_order", new SimpleStringSchema(), properties);
        DataStreamSource<String> kafkaSource = env.addSource(test);

        CheckpointConfig cpConfig = env.getCheckpointConfig();
        env.enableCheckpointing(5000);
        cpConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
        cpConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        StateBackend rocksDBStateBackend = new RocksDBStateBackend("hdfs:///checkpoints-data/");
        env.setStateBackend(rocksDBStateBackend);

        //设置task失败重试次数和重试间隔
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000));

        FlinkKafkaProducer010<String> myProducer = new FlinkKafkaProducer010<String>(
                "localhost:9092",
                "grampus_order_etl",
                new SimpleStringSchema());
        //\u0000
        SingleOutputStreamOperator<String> filter = kafkaSource.filter(x -> {
            String[] arr = x.split("#%&");
            String jsonData = arr[1];
            JSONObject dataJo = JSON.parseObject(jsonData);
            JSONArray columnsJo = dataJo.getJSONArray("columns");
            AtomicBoolean flag= new AtomicBoolean(true);
            columnsJo.forEach((Object jo) -> {
                JSONObject childJo = JSON.parseObject(jo.toString());
                if("order_status".equals(childJo.getString("name"))){
                    if(childJo.getInteger("value")!=1){
                        flag.set(false);
                    }
                }
            });
            return x.split("#@#", 2).length == 2 && flag.get();
        });

        SingleOutputStreamOperator<String> kafkaSinkStream = filter.map((MapFunction<String, String>) x -> {
            Character separator = '\u0000';
            //gms_order_2#@#gms_order_225#%&
            String[] arr = x.split("#@#");
//            String db = arr[0];
            String[] arr2 = arr[1].split("#%&");
//            String tb = arr2[0];
            /**
             * {"columns":[{"name":"store_code","update":false,"value":"BLDCQ2101835","key":false},{"name":"order_id","update":false,"value":"19022618440000652785","key":false},
             * "keys":[{"name":"id","update":true,"value":"12043","key":true}],
             * "type":"UPDATE"}
             */
            String jsonData = arr2[1];
            JSONObject dataJo = JSON.parseObject(jsonData);
            JSONArray columnsJo = dataJo.getJSONArray("columns");
            String id = JSON.parseObject(dataJo.getJSONArray("keys").getString(0)).getString("value");
            StringBuilder result = new StringBuilder(id).append(separator);
            columnsJo.forEach(jo -> {
                JSONObject childJo = JSON.parseObject(jo.toString());
                result.append(childJo.getString("value"));
                result.append(separator);
            });
            int i = result.lastIndexOf(separator.toString());
            return result.delete(i, i + 1).toString();
        });


        kafkaSinkStream.addSink(myProducer);

        env.execute();
    }
}
