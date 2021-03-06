package program;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
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
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;

/**
 * @author wangzhihua
 * @date 2019-03-14 15:57
 */
public class DoubleJoin {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        //设置kafka连接参数
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.2.40.10:9092,10.2.40.15:9092,10.2.40.14:9092");
        //properties.setProperty("flink.partition-discovery.interval-millis", "5000");
        properties.setProperty("group.id", "jj");
        properties.setProperty("auto.offset.reset", "earliest");
        FlinkKafkaConsumer<String> kafkaConsumer1 = new FlinkKafkaConsumer<>("join1", new SimpleStringSchema(), properties);
        kafkaConsumer1.setStartFromEarliest();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        //设置水印生成周期
        env.getConfig().setAutoWatermarkInterval(5000L);
        kafkaConsumer1.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<String>() {
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
            public long extractTimestamp(String element, long previousElementTimestamp) {
                String[] split = element.split(",");
                String timeStamp = split[0];
                String name = split[1];
                String city = split[2];
                Row row = new Row(3);
                row.setField(0,timeStamp);
                row.setField(1,name);
                row.setField(2,city);
                long timeStamp1 = 0;
                try {
                    timeStamp1 = simpleDateFormat.parse(timeStamp).getDate();
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                currentMaxTimestamp = Math.max(timeStamp1, currentMaxTimestamp);
                return timeStamp1 ;
            }
        });
        FlinkKafkaConsumer<String> kafkaConsumer2 = new FlinkKafkaConsumer<>("join2", new SimpleStringSchema(), properties);
        kafkaConsumer2.setStartFromEarliest();
        kafkaConsumer2.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<String>() {
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
            public long extractTimestamp(String element, long previousElementTimestamp) {
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
                long timeStamp1 = 0;
                try {
                    timeStamp1 = simpleDateFormat.parse(timeStamp).getDate();
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                currentMaxTimestamp = Math.max(timeStamp1, currentMaxTimestamp);
                return timeStamp1 ;
            }
        });
        DataStreamSource<String> source1 = env.addSource(kafkaConsumer1);
        DataStreamSource<String> source2 = env.addSource(kafkaConsumer2);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

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

        });

        source2.print();
        /**
         * 双流join
         */
        stream1.coGroup(stream2)
                .where(new KeySelector<Row, String>() {
                    @Override
                    public String getKey(Row value) throws Exception {
                        return value.getField(1).toString();
                    }
                })
                .equalTo(new KeySelector<Row, String>() {
                    @Override
                    public String getKey(Row value) throws Exception {
                        return value.getField(1).toString();
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                //.trigger(CountTrigger.of(5))
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
                                System.out.println("JOIN"+row.toString());
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
