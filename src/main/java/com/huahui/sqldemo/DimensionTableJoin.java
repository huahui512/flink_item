package com.huahui.sqldemo;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * @author wangzhihua
 * @date 2019-05-07 14:22
 */
public class DimensionTableJoin {
    public static void main(String[] args) throws Exception {
        System.out.println("===============》 flink任务开始  ==============》");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //设置任务重启的次数和间隔
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // 尝试重启次数
                Time.of(5, TimeUnit.SECONDS) // 延迟时间间隔
        ));
        //设置时间类型
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置检查点时间间隔
        env.enableCheckpointing(5000);
        //设置检查点模式
        DataStreamSource<String> streamSource = env.socketTextStream("127.0.0.1",6888);
        streamSource.writeAsText("hdfs://HDFS80377/testdata");
       /* streamSource.print();
        SingleOutputStreamOperator<String> result = JoinTable.RegisterJoinTable(streamSource, tableEnv, new JoinFlatFun("userinfo"));
        //获取join的结果流注册成表   表名  流表名+维表名
        tableEnv.registerDataStream("t1",result);*/
        env.execute("ddddddd");
    }

}
