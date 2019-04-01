package examples;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.*;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;


/**
 * @author wangzhihua
 * @date 2019-03-06 16:49
 */
public class Kafka2Kafka {
    private static final Logger LOGGER = LoggerFactory.getLogger(Kafka2Kafka.class);

    public static void main(String[] args) throws Exception {
      //  System.setProperty("HADOOP_USER_NAME","www");
        String kafkaBrokers = null;
        String zkBrokers = null;
        String topic = null;
        String groupId = null;
        String outTopic = null;
        if (args.length == 5) {
            kafkaBrokers = args[0];
            zkBrokers = args[1];
            topic = args[2];
            groupId = args[3];
            outTopic = args[4];
        } else {
            System.exit(1);
        }

        System.out.println("===============》 flink任务开始  ==============》");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ExecutionEnvironment env2 = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> stringDataSource = env2.fromElements("");
        //设置kafka连接参数
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaBrokers);
        properties.setProperty("zookeeper.connect", zkBrokers);
        properties.setProperty("flink.partition-discovery.interval-millis", "5000");
        properties.setProperty("group.id", groupId);

        //设置时间类型
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置检查点时间间隔
        env.enableCheckpointing(5000);
        //设置检查点模式
        //env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        System.out.println("===============》 开始读取kafka中的数据  ==============》");
        //创建kafak消费者，获取kafak中的数据
        FlinkKafkaConsumer010<String> kafkaConsumer010 = new FlinkKafkaConsumer010<>(topic, new SimpleStringSchema(), properties);

        kafkaConsumer010.setStartFromTimestamp(1553567497000L);
        DataStreamSource<String> kafkaData = env.addSource(kafkaConsumer010);
        //解析kafka数据流 转化成固定格式数据流
        DataStream<String> userData = kafkaData.map(new RichMapFunction<String, String>() {
            Counter mapDataNub;
            @Override
            public void open(Configuration parameters) throws Exception {
                mapDataNub=  getRuntimeContext()
                       .getMetricGroup()
                       .addGroup("flink_test_metric")
                       .counter("mapDataNub");

            }

            @Override
            public String map(String s)  {
                String s1 ="";
                try {
                    String[] split = s.split(",");
                    long userID = Long.parseLong(split[0]);
                    long itemId = Long.parseLong(split[1]);
                    long categoryId = Long.parseLong(split[2]);
                    String behavior = split[3];
                    long timestamp = Long.parseLong(split[4]);
                    Map map = new HashMap();
                    map.put("userID", userID);
                    map.put("itemId", itemId);
                    map.put("categoryId", categoryId);
                    map.put("behavior", behavior);
                    map.put("timestamp", timestamp);
                    s1 = JSON.toJSONString(map);
                    mapDataNub.inc();
                    System.out.println("数据"+map.toString());
                } catch (NumberFormatException e) {
                    e.printStackTrace();
                }
                return  s1;
            }


        });


        FlinkKafkaProducer010<String> myProducer = new FlinkKafkaProducer010<String>(
                kafkaBrokers,            // broker list
                outTopic,                  // target topic
                new SimpleStringSchema());   // serialization schema
        userData.addSink(myProducer);//参数分别是：写入topic，序列化器，kafka配置惨
        env.execute("data2es");
    }


    public static class MyMapper extends RichMapFunction<String,String>{
        private transient Counter counter;

        @Override
        public String map(String value) throws Exception {
            this.counter.inc();
            return value;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            //super.open(parameters);
            this.counter=getRuntimeContext()
                    .getMetricGroup()
                    .counter("MyCounter");
        }


        public static class FalconReporter implements MetricReporter, CharacterFilter, Scheduled {

            private static final Logger LOG =LoggerFactory.getLogger(FalconReporter.class);

            private final Map<Gauge<?>, MetricTag> gauges = new ConcurrentHashMap<>();
            private final Map<Counter, MetricTag> counters = new ConcurrentHashMap<>();
            private final Map<Histogram, MetricTag> histograms = new ConcurrentHashMap<>();
            private final Map<Meter, MetricTag> meters = new ConcurrentHashMap<>();

            @Override
            //用于对scope进行过滤.
            public String filterCharacters(String s) {
                return s;
            }

            @Override
            public void open(MetricConfig metricConfig) {

            }

            @Override
            public void close() {
            }

            @Override
            public void notifyOfAddedMetric(Metric metric, String s, MetricGroup metricGroup) {

            }

            @Override
            public void notifyOfRemovedMetric(Metric metric, String s, MetricGroup metricGroup) {
            }

            @Override
            public void report() {

            }
        }

    }
































}

