package examples;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.JoinedStreams;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Tuple;
import scala.Tuple3;

import javax.annotation.Nullable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;

import static sun.misc.Version.print;

/**
 * @author wangzhihua
 * @date 2019-03-14 15:57
 */
public class DoubleJoin {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //设置kafka连接参数
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.2.40.10:9092,10.2.40.15:9092,10.2.40.14:9092");
        //properties.setProperty("flink.partition-discovery.interval-millis", "5000");
        properties.setProperty("group.id", "jj");
        properties.setProperty("auto.offset.reset", "earliest");
        FlinkKafkaConsumer010<String> kafkaConsumer1 = new FlinkKafkaConsumer010<>("join1", new SimpleStringSchema(), properties);
        kafkaConsumer1.setStartFromEarliest();

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        kafkaConsumer1.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(1000L)) {

            @Override
            public long extractTimestamp(String element) {
                String[] split = element.split(",");
                String timeStamp = split[0];
                String name = split[1];
                String city = split[2];
                Row row = new Row(3);
                row.setField(0,timeStamp);
                row.setField(1,name);
                row.setField(2,city);
                System.out.println(row.toString());
                long timeStamp1 = 0;
                try {
                    timeStamp1 = simpleDateFormat.parse(timeStamp).getDate();
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                return timeStamp1 ;
            }


        });
        FlinkKafkaConsumer010<String> kafkaConsumer2 = new FlinkKafkaConsumer010<>("join2", new SimpleStringSchema(), properties);
        kafkaConsumer2.setStartFromEarliest();
        kafkaConsumer2.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(1000L)) {
            @Override
            public long extractTimestamp(String element) {
                String[] split = element.split(",");
                String timeStamp = split[0];
                String name = split[1];
                String age = split[2];
                String school= split[3];
                Row row = new Row(4);
                row.setField(0,timeStamp);
                row.setField(1,name);
                row.setField(2,age);
                row.setField(3,school);
                System.out.println(row.toString());
                long timeStamp1 = 0;
                try {
                    timeStamp1 = simpleDateFormat.parse(timeStamp).getDate();
                } catch (ParseException e) {
                    e.printStackTrace();
                }
               // currentMaxTimestamp = Math.max(timeStamp1, currentMaxTimestamp);
                return timeStamp1 ;
            }


        });
        DataStreamSource<String> source1 = env.addSource(kafkaConsumer1);
        DataStreamSource<String> source2 = env.addSource(kafkaConsumer2);

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
        });

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

        });
        /**
         * 双流join
         */
        stream1.coGroup(stream2)
                .where(new KeySelector<Row, String>() {
                    @Override
                    public String getKey(Row value) throws Exception {
                        System.out.println("stream1---->"+value.toString());
                        return value.getField(1).toString();
                    }
                })
                .equalTo(new KeySelector<Row, String>() {
                    @Override
                    public String getKey(Row value) throws Exception {
                        System.out.println("stream2--->"+value.toString());
                        return value.getField(1).toString();
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new CoGroupFunction<Row, Row, String>() {
                    @Override
                    public void coGroup(Iterable<Row> first, Iterable<Row> second, Collector<String> out) throws Exception {
                        System.out.println("流1"+first+"流2"+second);
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
                                out.collect(row.toString());
                                System.out.println(row.toString()+"yyyyyyyy");
                            }
                    ));
                    }
                }).print();

        try {
            env.execute("ddcddd");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
