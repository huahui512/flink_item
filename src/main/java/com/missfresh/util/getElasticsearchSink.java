/*
package com.missfresh.util;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


*/
/**
 * @author wangzhihua
 * @date 2019-01-22 18:07
 *//*


class FlinkElastic {

    public static ElasticsearchSink getElasticsearchSink(String esTransPorts, String clusterName) {
        ElasticsearchSink esSink = null;
        Map<String, String> config = new HashMap<String, String>();

        // this instructs the sink to emit after every element, otherwise they would be buffered
        config.put(ElasticsearchSink.CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS, "3000");
        config.put(ElasticsearchSink.CONFIG_KEY_BULK_FLUSH_INTERVAL_MS, "1");
        config.put("cluster.name", clusterName);

        try {
            List<InetSocketAddress> transports = new ArrayList<InetSocketAddress>();
            // port is 9300 for elastic transportClient
//            transports.add(new InetSocketAddress("11.11.184.182", 9300));
            for (String s : esTransPorts.split(",")) {
                String[] transPort = s.split(":");
                transports.add(new InetSocketAddress(transPort[0], Integer.parseInt(transPort[1])));
            }

            ElasticsearchSinkFunction<JSONObject> indexLog = new ElasticsearchSinkFunction<JSONObject>() {
                public IndexRequest createIndexRequest(JSONObject elements) {
                    String log_type = elements.getString("log_type");
                    */
/*final DateTime dateTime = new DateTime(elements.getLongValue("recv_time"));
                    String indexPrefix = dateTime.toString("yyyyMMdd");*//*

                    return Requests.indexRequest().index(" " + log_type).type(log_type).source(elements);
                }

                @Override
                public void process(JSONObject s, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                    requestIndexer.add(createIndexRequest(s));

                }
            };
            esSink = new ElasticsearchSink(config, transports, indexLog);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return esSink;
    }
}
*/
