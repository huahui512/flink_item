package examples;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.JoinedStreams;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Tuple;
import scala.Tuple3;

import javax.annotation.Nullable;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import static sun.misc.Version.print;

/**
 * @author wangzhihua
 * @date 2019-03-14 15:57
 */
public class DoubleJoin {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source1 = env.readTextFile("/Users/apple/Downloads/1.txt");
        DataStreamSource<String> source2 = env.readTextFile("/Users/apple/Downloads/2.txt");
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        /**
         * 数据流1
         */
        SingleOutputStreamOperator<Row> stream1 = source1.map(new MapFunction<String, Row>() {
            @Override
            public Row map(String value) throws Exception {
                String[] split = value.split(",");
                String timeStamp = split[0];
                String name = split[1];
                String city = split[2];
                Row row = new Row(3);
                row.setField(0,timeStamp);
                row.setField(1,name);
                row.setField(2,city);
                return row;
            }
        }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Row>() {
             long  currentMaxTimestamp = 0L;
             long  maxOutOfOrderness = 10000L;
             Watermark watermark=null;
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
                     return timeStamp ;
             }
         }
        );
        stream1.print();
        /**
         * 数据流2
         */
        SingleOutputStreamOperator<Row> stream2 = source2.map(new MapFunction<String, Row>() {
            @Override
            public Row map(String value) throws Exception {
                String[] split = value.split(",");
                String timeStamp = split[0];
                String name = split[1];
                String age = split[2];
                String school= split[3];
                Row row = new Row(4);
                row.setField(0,timeStamp);
                row.setField(1,name);
                row.setField(2,age);
                row.setField(3,school);
                return row;
            }

        }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Row>() {
            long  currentMaxTimestamp = 0L;
            long  maxOutOfOrderness = 10000L;
            Watermark watermark=null;
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
                return timeStamp ;
            }
        });
         stream2.print();
        /**
         * 双流join
         */
        stream1.coGroup(stream2)
                .where(new KeySelector<Row, String>() {
                    @Override
                    public String getKey(Row value) throws Exception {
                        System.out.println("stream1"+value.toString());
                        return value.getField(1).toString();
                    }
                })
                .equalTo(new KeySelector<Row, String>() {
                    @Override
                    public String getKey(Row value) throws Exception {
                        System.out.println("stream2"+value.toString());
                        return value.getField(1).toString();
                    }
                }).window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new CoGroupFunction<Row, Row, Row>() {
                    @Override
                    public void coGroup(Iterable<Row> first, Iterable<Row> second, Collector<Row> out) throws Exception {
                    first.forEach(t ->
                    second.forEach(x ->
                            {
                                //双流join  选取需要的字段
                                Row row = new Row(3);
                                Object field1 = t.getField(0);
                                Object field2 = x.getField(1);
                                Object field3 = x.getField(2);
                                //使用row封装数据
                                row.setField(0, field1);
                                row.setField(1, field2);
                                row.setField(2, field3);
                                out.collect(row);
                            }
                    ));
                    System.out.println("join"+first.toString());
                    }
                }).printToErr();

        try {
            env.execute("ddcddd");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
