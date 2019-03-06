package com.missfresh.output;

import com.missfresh.util.SqlParse;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

/**
 * @author wangzhihua
 * @date 2019-03-04 15:32
 */
public class EsOutPut extends RichSinkFunction<Row> {
    String sqlInfo;

    public EsOutPut(String sqlInfo) {
        this.sqlInfo = sqlInfo;
    }

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

    /**
     * @param value
     * @param context
     * @throws Exception
     */

    @Override
    public void invoke(Row value, Context context) throws Exception {
        //根据用户传入的sql，设置es的字段名
        Map map = SqlParse.sqlParse(sqlInfo, value);
        // 添加单次请求
        bulkProcessor.add(new IndexRequest("sql_flink1", "user_info1").source(map));

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
