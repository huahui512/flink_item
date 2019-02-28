package examples;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;


import java.util.*;

/**
 * @author wangzhihua
 * @date 2019-01-14 16:35
 */
public class Data2Es {
    public static void main(String[] args) throws Exception {
        String kafkaBrokers = null;
        String zkBrokers = null;
        String topic = null;
        String groupId = null;
        if (args.length == 4) {
            kafkaBrokers = args[0];
            zkBrokers = args[1];
            topic = args[2];
            groupId = args[3];
        } else {
            System.exit(1);
        }
        System.out.println("===============》 flink任务开始  ==============》");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置kafka连接参数
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaBrokers);
        properties.setProperty("zookeeper.connect", zkBrokers);
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
        kafkaConsumer010.setStartFromEarliest();
        DataStreamSource<String> kafkaData = env.addSource(kafkaConsumer010);
        kafkaData.print();
        //解析kafka数据流 转化成固定格式数据流
        DataStream<Tuple5<Long, Long, Long, String, Long>> userData = kafkaData.map(new MapFunction<String, Tuple5<Long, Long, Long, String, Long>>() {
            @Override
            public Tuple5<Long, Long, Long, String, Long> map(String s) throws Exception {
                String[] split = s.split(",");
                    long userID = Long.parseLong(split[0]);
                    long itemId = Long.parseLong(split[1]);
                    long categoryId = Long.parseLong(split[2]);
                    String behavior = split[3];
                    long timestamp = Long.parseLong(split[4]);
                    System.out.println("&&&&"+behavior);
                    Tuple5<Long, Long, Long, String, Long> userInfo = new Tuple5<>(userID, itemId, categoryId, behavior, timestamp);
                    return userInfo;
            }
        });

        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("10.2.40.15", 9200, "http"));
        httpHosts.add(new HttpHost("10.2.40.10", 9200, "http"));
        httpHosts.add(new HttpHost("10.2.40.14", 9200, "http"));
       // use a ElasticsearchSink.Builder to create an ElasticsearchSink
        ElasticsearchSink.Builder<Tuple5<Long, Long, Long, String, Long>> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<Tuple5<Long, Long, Long, String, Long>>() {
                    public IndexRequest createIndexRequest(Tuple5<Long, Long, Long, String, Long> element) {
                        Map<String, String> json = new HashMap<>();
                        json.put("userid", element.f0.toString());
                        json.put("itemid", element.f1.toString());
                        json.put("categoryid", element.f2.toString());
                        json.put("behavior", element.f3);
                        json.put("timestamp", element.f4.toString());

                        return Requests.indexRequest()
                                .index("user_info")
                                .type("flink_test")
                                .source(json);
                    }
                    @Override
                    public void process(Tuple5<Long, Long, Long, String, Long> element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );

        //必须设置flush参数
        esSinkBuilder.setBulkFlushMaxActions(1);

        userData.addSink(esSinkBuilder.build());

        env.execute("data2es");
    }
}