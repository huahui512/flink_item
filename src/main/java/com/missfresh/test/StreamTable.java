package com.missfresh.test;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class StreamTable {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        DataStreamSource<String> dss = env.socketTextStream("localhost", 9999, "\n");
        SingleOutputStreamOperator<Tuple2<String, String>> tuple2 = dss.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                String[] split = value.split(",");
                return new Tuple2<String, String>(split[0], split[1]);
            }
        });


//        Table table = tableEnv.fromDataStream(dss, "user,url");
        tableEnv.registerDataStream("abc", tuple2, "user,url");

//        tableEnv.registerTable("abc", table);
//        Table table = tableEnv.sqlQuery("SELECT * FROM abc ");
        Table table = tableEnv.sqlQuery("SELECT user, COUNT(url) FROM abc GROUP BY user");
        DataStream rowDataStream = tableEnv.toRetractStream(table, Row.class);

        rowDataStream.map(new MapFunction() {
            @Override
            public Object map(Object value) throws Exception {
                return null;
            }
        });

        rowDataStream.print();
        env.execute("");
    }
}
