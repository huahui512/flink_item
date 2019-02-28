package com.missfresh.test;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * @author wangtaiyang
 */
public class TableStreamConsumerKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test8");
//        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("auto.offset.reset", "latest");
        FlinkKafkaConsumer010<String> kafkaStream = new FlinkKafkaConsumer010<>("test", new SimpleStringSchema(), properties);
        DataStreamSource<String> kafkaStreamSource = env.addSource(kafkaStream);

        CheckpointConfig cpConfig = env.getCheckpointConfig();
        env.enableCheckpointing(5000);
        cpConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
        cpConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        StateBackend rocksDBStateBackend = new RocksDBStateBackend("hdfs:///checkpoints-data/");
        env.setStateBackend(rocksDBStateBackend);


        String[] names = new String[]{"user", "url"};
        TypeInformation[] types = new TypeInformation[]{Types.STRING(), Types.INT()};

        RowTypeInfo rowTypeInfo = new RowTypeInfo(types, names);
        SingleOutputStreamOperator<Row> userData = kafkaStreamSource.map(new MapFunction<String, Row>() {
            @Override
            public Row map(String s) {
                String[] split = s.split(",");
                Row row = new Row(split.length);
                for (int i = 0; i < split.length; i++) {
                    if(i==0){
                        row.setField(i, split[i]);
                    }else{
                        row.setField(i,Integer.valueOf(split[i]));
                    }
                }

                return row;
            }
        }).returns(rowTypeInfo);

        tableEnv.registerDataStream("test",userData);

        String sql="select user,sum(url) from test group by user";
        Table t = tableEnv.sqlQuery(sql);

        tableEnv.toRetractStream(t, Row.class).map((MapFunction<Tuple2<Boolean, Row>, String>) value -> {
            return null;
        });


        env.execute("aaa");

    }
}
