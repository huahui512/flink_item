package examples;

import com.missfresh.source.MysqlSource;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.types.Row;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author wangzhihua
 * @date 2019-03-18 16:43
 */
public class DimensionTablejoin {
    public static void main(String[] args) throws Exception {
        System.out.println("===============》 flink任务开始  ==============》");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source1 = env.addSource(new MysqlSource());
        source1.print();
        //设置kafka连接参数
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.2.40.10:9092,10.2.40.15:9092,10.2.40.14:9092");
        //properties.setProperty("flink.partition-discovery.interval-millis", "5000");
        properties.setProperty("group.id", "jj");
        properties.setProperty("auto.offset.reset", "earliest");
        FlinkKafkaConsumer010<String> kafkaConsumer1 = new FlinkKafkaConsumer010<>("flink_log3", new SimpleStringSchema(), properties);
        kafkaConsumer1.setStartFromEarliest();
        DataStreamSource<String> kafkaData = env.addSource(kafkaConsumer1);
        SingleOutputStreamOperator<Row> rowSource = kafkaData.map(new MapFunction<String, Row>() {
            @Override
            public Row map(String value) throws Exception {
                String[] split = value.split(",");
                String name = split[0];
                String city = split[1];
                Row row = new Row(2);
                row.setField(0, name);
                row.setField(1, city);
                return row;
            }
        });
       // rowSource.print();
       DataStream<String> result = rowSource.join(source1)
                .where(new KeySelector<Row, String>() {
                    @Override
                    public String getKey(Row value) throws Exception {
                        return value.getField(0).toString();
                    }
                })
                .equalTo(new KeySelector<String, String>() {
                    @Override
                    public String getKey(String value) throws Exception {

                        return value;
                    }
                }).window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
               .trigger(CountTrigger.of(5))
                .apply(new JoinFunction<Row, String, String>() {
                    @Override
                    public String join(Row first, String second) throws Exception {
                        System.out.println(first+"==================");
                        return first.getField(1) + second+"join";
                    }
                });

        result.print();
        env.execute("jjjj");
    }
}
