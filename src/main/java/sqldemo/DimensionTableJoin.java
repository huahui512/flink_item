package sqldemo;

import com.missfresh.source.MysqlSource;
import com.missfresh.source.MysqlSource2;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
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


       /* streamSource.print();
        SingleOutputStreamOperator<String> result = JoinTable.RegisterJoinTable(streamSource, tableEnv, new JoinFlatFun("userinfo"));
        //获取join的结果流注册成表   表名  流表名+维表名
        tableEnv.registerDataStream("t1",result);*/
        env.execute("ddddddd");
    }

}
