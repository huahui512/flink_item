package examples;


import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Map;

/**
 * @author wangzhihua
 * @date 2019-04-03 19:43
 */
public class EsBulkUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(EsBulkUtil.class);
    private static  RestHighLevelClient  client;
    private static  BulkProcessor bulkProcessor;

    public EsBulkUtil() {
    }
    /**
     * 获取client连接
     */
    public  void  initHighLevelClient(String host, int port, String schema) {
        if (client == null) {
            synchronized (EsBulkUtil.class) {
                if (client == null) {
                    LOGGER.info("es create connection");
                    String[] ipArr = host.split(",");
                    HttpHost[] httpHosts = new HttpHost[ipArr.length];
                    for (int i = 0; i < ipArr.length; i++) {
                        httpHosts[i] = new HttpHost(ipArr[i], port, schema);
                    }
                    RestClientBuilder builder = RestClient.builder(httpHosts);
                    this.client = new RestHighLevelClient(builder);
                    LOGGER.info("es create connection success");
                }
            }
        }
    }
    /**
     * 获取BulkProcessor操作对象
     * @param
     * @return
     */
    public void initBulkProcessor() {

        Settings settings = Settings.builder().build();
        ThreadPool threadPool = new ThreadPool(settings);
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                //重写beforeBulk,在每次bulk request发出前执行,在这个方法里面可以知道在本次批量操作中有多少操作数
                int numberOfActions = request.numberOfActions();
                LOGGER.debug("Executing bulk [{" + executionId + "}] with {" + numberOfActions + "} requests");
            }
            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                //重写afterBulk方法，每次批量请求结束后执行，可以在这里知道是否有错误发生
                if (response.hasFailures()) {
                LOGGER.warn("Bulk [{" + executionId + "}] executed with failures");
                } else {
                 LOGGER.debug("Bulk [{" + executionId + "}] completed in {" + response.getTook().getMillis() + "} milliseconds");
                }
            }
            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                //重写方法，如果发生错误就会调用。
                LOGGER.error("Failed to execute bulk", failure);
                failure.printStackTrace();
            }
        };

        BulkProcessor.Builder BulkProcessorBuilder = new BulkProcessor.Builder(client::bulkAsync, listener, threadPool);
        //1w次请求执行一次bulk
        BulkProcessorBuilder.setBulkActions(100000);
        //64MB的数据刷新一次bulk
        BulkProcessorBuilder.setBulkSize(new ByteSizeValue(64L, ByteSizeUnit.MB));
        //并发请求数量，0不并发，1并发允许执行
        BulkProcessorBuilder.setConcurrentRequests(0);
        //固定60s刷新一次
        BulkProcessorBuilder.setFlushInterval(TimeValue.timeValueSeconds(60L));
        //设置退避,1s后执行，最大请求3次
        BulkProcessorBuilder.setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(1L), 3));
        //在这里调用build()方法构造bulkProcessor,在底层实际上是用了bulk的异步操作
        bulkProcessor = BulkProcessorBuilder.build();
    }
    /**
     * 插入数据
     * @return
     */
    public  void  bulkAdd(String indexName, String typeName, String indexId, Map<String, String> map) {
        try {
            IndexRequest indexRequest = new IndexRequest(indexName, typeName, indexId)
                    .source(map);
            UpdateRequest updateRequest = new UpdateRequest(indexName, typeName, indexId)
                    .doc(map)
                    .upsert(indexRequest);
            bulkProcessor.add(updateRequest);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    /**
     * 删除数据
     * @return
     */
    public  void  bulkDelete(String indexName, String typeName, String indexId) {
        try {
            bulkProcessor.add(new DeleteRequest(indexName, typeName, indexId));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    /**
     * 刷新bulkProcessor
     */
    public void flush() {
        try {
            bulkProcessor.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    /**
     * 关闭连接
     */
    public  void close() {
        if (client != null) {
            try {
                bulkProcessor.flush();
                bulkProcessor.close();
                client.close();

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
