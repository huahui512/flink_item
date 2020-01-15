package program;



import com.huahui.util.GetInfo;
import com.huahui.util.MyRowInfo;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;


import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author wangzhihua
 * @date 2019-01-14 16:35
 */
public class Data2Es {
    public static void main(String[] args) throws Exception {
        String kafkaBrokers = null;
        String zkBrokers = null;
        String topic = null;
        String groupId = null;
        String sqlInfo = null;
        String tableName = null;
        String offsetType = null;
        String interval = null;
        //判断参数输入是否完整
        if (args.length == 8) {
            kafkaBrokers = args[0];
            zkBrokers = args[1];
            topic = args[2];
            groupId = args[3];
            sqlInfo = args[4];
            tableName = args[5];
            offsetType = args[6];
            interval = args[7];
        } else {
            System.exit(1);
        }
        System.out.println("===============》  flink任务开始  =================》");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置kafka连接参数
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaBrokers);
        properties.setProperty("zookeeper.connect", zkBrokers);
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", offsetType);
        //设置时间类型
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置检查点时间间隔
        env.enableCheckpointing(Long.parseLong(interval));
        //设置任务重启的次数和间隔
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(5, TimeUnit.SECONDS)));
        //设置检查点模式
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //获取表对象
        EnvironmentSettings setting = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,setting);  System.out.println("===============》 开始读取kafka中的数据  ==============》");
        //创建kafak消费者，获取kafak中的数据
        FlinkKafkaConsumer<String> kafkaConsumer010 = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        SingleOutputStreamOperator<String> kafkaData = env.addSource(kafkaConsumer010).uid("source1");
        //获取表的字段名和类型
        String typeInfo = GetInfo.getTypeInfo();
        //设置Row的字段名称和类型
        RowTypeInfo rowTypeInfo = MyRowInfo.getRowTypeInfo(typeInfo);
        //获取字段对应的数据类型
        TypeInformation[] typeInformations = rowTypeInfo.getFieldTypes();
        //使用Row封装数据类型
        //通过循环 获取每一个字段的值
        SingleOutputStreamOperator<Row> userData = kafkaData.map((MapFunction<String, Row>) s -> {
            String[] split = s.split("\u0000");
            Row row = new Row(split.length);
            for (int i = 0; i < split.length; i++) {
                String typeStr = typeInformations[i].toString();
                if ("Integer".equals(typeStr)) {
                    row.setField(i, Integer.valueOf(split[i]));
                } else if ("Long".equals(typeStr)) {
                    row.setField(i, Long.parseLong(split[i]));
                } else if ("Double".equals(typeStr)) {
                    row.setField(i, Double.parseDouble(split[i]));
                } else if ("Float".equals(typeStr)) {
                    row.setField(i, Float.parseFloat(split[i]));
                } else {
                    row.setField(i, String.valueOf(split[i]));
                }
            }
            return row;

        }).uid("map1")
                //返回Row封装数据的名称与类型,以便下一个算子能识别此类型
                .returns(rowTypeInfo).uid("return1");
        //实时流转化成表
        Table table = tableEnv.fromDataStream(userData);
        //把实时流注册成表
        tableEnv.registerDataStream(tableName, userData);
        //进行sql查询生成表对象
        Table table2 = tableEnv.sqlQuery(sqlInfo);
        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tableEnv.toRetractStream(table2, Row.class);
        SingleOutputStreamOperator<Row> outputStream = tuple2DataStream.map((MapFunction<Tuple2<Boolean, Row>, Row>) value -> {
            Row row1 = value.f1;
            return row1;
        }).uid("map2");
        //数据写入es
       // outputStream.addSink(new EsOutPut(sqlInfo)).uid("sink1");
        //设置jobName
        env.execute("data2es");
    }
}