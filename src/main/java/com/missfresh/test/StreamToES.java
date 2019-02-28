package com.missfresh.test;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;

import java.net.InetAddress;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author wangtaiyang
 */
public class StreamToES {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test3");
        properties.setProperty("auto.offset.reset", "earliest");
        FlinkKafkaConsumer010<String> test = new FlinkKafkaConsumer010<>("test", new SimpleStringSchema(), properties);
        DataStreamSource<String> stringDataStreamSource = env.addSource(test);

        CheckpointConfig cpConfig = env.getCheckpointConfig();
        env.enableCheckpointing(5000);
        cpConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
        cpConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        StateBackend rocksDBStateBackend = new RocksDBStateBackend("hdfs:///checkpoints-data/");
        env.setStateBackend(rocksDBStateBackend);

        stringDataStreamSource.addSink(new BulkToES());

        env.execute("aaa");
    }

    public static class BulkToES extends RichSinkFunction<String> {
        TransportClient client;
        BulkProcessor bulkProcessor;
        BulkRequestBuilder bulkRequest;

        @Override
        public void open(Configuration parameters) throws Exception {

            Settings settings = Settings.builder()
                    .put("xpack.security.user", "elastic:changeme")
                    .put("cluster.name", "my-es")
                    .put("xpack.security.transport.ssl.enabled", false)
                    .put("client.transport.sniff", true)
                    .build();
            client = new PreBuiltXPackTransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));

            bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
                /**
                 * Callback before the bulk is executed.
                 */
                @Override
                public void beforeBulk(long l, BulkRequest bulkRequest) {
                    System.out.println("beforeBulk");
                }

                /**
                 * Callback after a failed execution of bulk request.
                 * <p>
                 * Note that in case an instance of <code>InterruptedException</code> is passed, which means that request processing has been
                 * cancelled externally, the thread's interruption status has been restored prior to calling this method.
                 */
                @Override
                public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {
                    System.out.println("请求成功时的回调");
                }

                /**
                 * Callback after a successful execution of bulk request.
                 */
                @Override
                public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {
                    System.out.println("请求失败后调用 "+l);
                    throwable.printStackTrace();
                }
            })
                    // 1w次请求执行一次bulk
                    .setBulkActions(10000)
                    // 1gb的数据刷新一次bulk
                    .setBulkSize(new ByteSizeValue(1, ByteSizeUnit.GB))
                    // 固定5s必须刷新一次
                    .setFlushInterval(TimeValue.timeValueSeconds(5))
                    // 并发请求数量, 0不并发, 1并发允许执行
                    .setConcurrentRequests(1)
                    // 设置退避, 100ms后执行, 最大请求3次
                    .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                    .build();
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            Map<String, Object> jsonMap = new HashMap<>(3);
            jsonMap.put("user", "kimchy");
            jsonMap.put("postDate", new Date());
            jsonMap.put("message", "me trying out Elasticsearch");

            Map<String, Object> jsonMap2 = new HashMap<>(3);
            jsonMap2.put("user", "upsert");
            jsonMap2.put("postDate", new Date());
            jsonMap2.put("message", "upsert-message");

            // 添加单次请求
            bulkProcessor.add(new IndexRequest("twitter", "tweet").source(jsonMap));
            if ("delete".equals(value)) {
                bulkProcessor.add(new DeleteRequest("twitter", "tweet", "1"));
            }
            if ("upsert".equals(value)) {
                bulkProcessor.add(new UpdateRequest("twitter", "tweet", "1").doc(jsonMap2));
                bulkProcessor.add(new UpdateRequest("twitter", "tweet", "1").doc(jsonMap2));
            }


        }

        @Override
        public void close() {
            System.out.println("close");
            // 关闭
            // bulkProcessor.awaitClose(10, TimeUnit.MINUTES);
            // 或者
            bulkProcessor.close();
        }
    }

    public static class SingleToES extends RichSinkFunction<String> {
        TransportClient client;

        @Override
        public void open(Configuration parameters) throws Exception {
            Settings settings = Settings.builder()
                    .put("xpack.security.user", "elastic:changeme")
                    .put("cluster.name", "my-es")
                    .put("xpack.security.transport.ssl.enabled", false)
                    .put("client.transport.sniff", true)
                    .build();
            client = new PreBuiltXPackTransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
        }


        @Override
        public void invoke(String value, Context context) throws Exception {
            XContentBuilder source = XContentFactory.jsonBuilder()
                    .startObject()
                    .field("user", value)
                    .field("postDate", new Date())
                    .field("message", "trying to out ElasticSearch")
                    .endObject();
            // 存json入索引中
            IndexResponse response = client.prepareIndex("twitter", "tweet", "1").setSource(source).get();
//        // 结果获取
            String index = response.getIndex();
            String type = response.getType();
            String id = response.getId();
            long version = response.getVersion();
            System.out.println(index + " : " + type + ": " + id + ": " + version);

        }

        @Override
        public void close() throws Exception {
            client.close();
        }
    }

}
