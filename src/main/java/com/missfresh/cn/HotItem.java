package com.missfresh.cn;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


/**
 * @author wangzhihua
 * @date 2018-12-25 11:05
 */
public class HotItem {
    public static void main(String[] args) throws Exception {
        // 创建 execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 告诉系统按照 EventTime 处理
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 为了打印到控制台的结果不乱序，我们配置全局的并发为1，改变并发对结果正确性没有影响
        env.setParallelism(1);
       // String filEPath = HotItem.class.getClass().getClassLoader().getResource("userData.txt").getPath();
        //读取hdfs中的测试数据
        DataStream<String> source = env.readTextFile("hdfs://HDFS80377/DimensionTablejoin/user.txt");
        source.print();
        System.out.println(source);
        SingleOutputStreamOperator<Tuple5<Long, Long, Long, String, Long>> data1 = source.map(new MapFunction<String, Tuple5<Long, Long, Long, String, Long>>() {
            @Override
            public Tuple5<Long, Long, Long, String, Long> map(String s) throws Exception {
                String[] split = s.split(",");
                long userID = Long.parseLong(split[0]);
                long itemId = Long.parseLong(split[1]);
                long categoryId = Long.parseLong(split[2]);
                String behavior = split[3];
                long timestamp = Long.parseLong(split[4]);
                Tuple5<Long, Long, Long, String, Long> userInfo = new Tuple5<>(userID, itemId, categoryId, behavior, timestamp);
                return userInfo;
            }
        });

        SingleOutputStreamOperator<Tuple5<Long, Long, Long, String, Long>> data2 = data1.filter(new FilterFunction<Tuple5<Long, Long, Long, String, Long>>() {
            @Override
            public boolean filter(Tuple5<Long, Long, Long, String, Long> Tuple5) throws Exception {
                return Tuple5.f3.equals("pv");
            }
        });

        SingleOutputStreamOperator<Tuple5<Long, Long, Long, String, Long>> data3 = data2
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple5<Long, Long, Long, String, Long>>() {
            @Override
            public long extractAscendingTimestamp(Tuple5<Long, Long, Long, String, Long> Tuple5) {
                return Tuple5.f4 * 1000;
            }
        });
        KeyedStream<Tuple5<Long, Long, Long, String, Long>, Tuple> keyByData = data3.keyBy(1);
        WindowedStream<Tuple5<Long, Long, Long, String, Long>, Tuple, TimeWindow> WindowedStream = keyByData.timeWindow(Time.minutes(60), Time.minutes(5));
        SingleOutputStreamOperator<ItemViewCount> aggregateData = WindowedStream.aggregate(new CountAgg(), new WindowResult());
        KeyedStream<ItemViewCount, Long> keyedStream = aggregateData.keyBy(new KeySelector<ItemViewCount, Long>() {
            @Override
            public Long getKey(ItemViewCount itemViewCount) throws Exception {
                return itemViewCount.windowEnd;
            }
        });
        System.out.println("任务开始");
        keyedStream.print();
        System.out.println("任务结束");

        env.execute("hot item");

    }
}
