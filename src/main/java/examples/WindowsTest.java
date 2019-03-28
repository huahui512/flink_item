package examples;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.types.Row;

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
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        System.out.println("============》 任务开始+++++");

        DataStreamSource<String> source1 = env.socketTextStream("127.0.0.1", 7777);
        SingleOutputStreamOperator<Row> stream1 = source1.map(new MapFunction<String, Row>() {
            @Override
            public Row map(String value) throws Exception {
                String[] split = value.split(",");
                String timeStamp = split[0];
                String name = split[1];
                int score = Integer.parseInt(split[2]);
                Row row = new Row(3);
                row.setField(0, timeStamp);
                row.setField(1, name);
                row.setField(2, score);
                System.out.println(row.toString());
                return row;
            }
        }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Row>() {
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
        );
        stream1.keyBy(new KeySelector<Row, String>() {
            @Override
            public String getKey(Row value) throws Exception {
                return value.getField(1).toString();
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .trigger(CountTrigger.of(1))
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
                })
                .print();

        try {
            env.execute("test");

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static class MyTrigger extends Trigger{
        @Override
        public TriggerResult onElement(Object element, long timestamp, Window window, TriggerContext ctx) throws Exception {
            return null;
        }

        @Override
        public TriggerResult onProcessingTime(long time, Window window, TriggerContext ctx) throws Exception {
            return null;
        }

        @Override
        public TriggerResult onEventTime(long time, Window window, TriggerContext ctx) throws Exception {
            return null;
        }

        @Override
        public void clear(Window window, TriggerContext ctx) throws Exception {

        }
    }
}