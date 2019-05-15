package examples;

import com.missfresh.util.JedisPoolUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import javax.annotation.Nullable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;

/**
 * @author wangzhihua
 * @date 2019-03-27 10:42
 */
public class WindowsTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //设置kafka连接参数
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","10.2.40.10:9092,10.2.40.15:9092,10.2.40.14:9092");
        properties.setProperty("group.id", "tt1");
        FlinkKafkaConsumer010<String> kafkaConsumer1 = new FlinkKafkaConsumer010<>("join1", new SimpleStringSchema(), properties);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        System.out.println("============》 任务开始+++++");
        //kafkaConsumer1.setStartFromEarliest();
        DataStreamSource<String> source1 = env.addSource(kafkaConsumer1).setParallelism(1);
        source1.print();

        /*SingleOutputStreamOperator<Row> stream1 = source1.map(new MapFunction<String, Row>() {
            @Override
            public Row map(String value) throws Exception {
                Row row = null;
                try {
                    String[] split = value.split(",");
                    if (split.length==3){
                    String timeStamp = split[0];
                    String name = split[1];
                    String score = split[2];
                    row = new Row(3);
                    row.setField(0, timeStamp);
                    row.setField(1, name);
                    row.setField(2, score);

                    JedisPoolUtil jedisPoolUtil = JedisPoolUtil.getInstance();
                    jedisPoolUtil.initialPool();
                    Jedis jedis = jedisPoolUtil.getJedis();
                    jedis.hset(score,name,timeStamp);
                    jedisPoolUtil.closePool();
                    jedisPoolUtil.returnResource(jedis);

                    }
                    else {
                        System.out.println(value);
                    }
                    //System.out.println(row.toString());
                } catch (NumberFormatException e) {
                    //e.printStackTrace();

                   System.out.println(value);
                }
                return row;
            }
        }).uid("ee").name("rrr").setParallelism(1);

*/



        /*.setParallelism(1).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Row>() {
                 long currentMaxTimestamp = 0L;
                 long maxOutOfOrderness = 10000L;
                 Watermark watermark = null;
            //最大允许的乱序时间是10s
                 @Nullable
                 @Override
                 public Watermark getCurrentWatermark() {
                     watermark = new Watermark(currentMaxTimestamp - maxOutOfOrderness);
                     return watermark;
                 }
                 @Override
                 public long extractTimestamp(Row element, long previousElementTimestamp) {
                     long timeStamp = 0;
                     try {
                         timeStamp = simpleDateFormat.parse(element.getField(0).toString()).getDate();
                     } catch (ParseException e) {
                         e.printStackTrace();
                     }
                     currentMaxTimestamp = Math.max(timeStamp, currentMaxTimestamp);
                     return timeStamp;
                 }
             }
        ).setParallelism(1);
        stream1.print().setParallelism(1);
        stream1.windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .trigger(CountTrigger.of(8))
                .apply(new AllWindowFunction<Row, String, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<Row> values, Collector<String> out) throws Exception {
                        values.forEach(t->
                                out.collect("排序后"+t.toString())
                                );

                    }
                }).setParallelism(1)
                .print();*/
       /* stream1.keyBy(new KeySelector<Row, String>() {
            @Override
            public String getKey(Row value) throws Exception {
                return value.getField(1).toString();
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(5)))
              // .trigger(CountTrigger.of(5))
                .reduce(new ReduceFunction<Row>() {
                    @Override
                    public Row reduce(Row value1, Row value2) throws Exception {
                        String s1 = value1.getField(2).toString();
                        String s2 = value2.getField(2).toString();
                        if (Integer.parseInt(s1) < Integer.parseInt(s2)) {
                            return value2;
                        } else {
                            return value1;
                        }
                    }
                }).print();*/
        try {
            env.execute("DimensionTablejoin");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}