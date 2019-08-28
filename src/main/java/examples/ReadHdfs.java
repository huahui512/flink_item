package examples;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author wangzhihua
 * @date 2019-04-23 16:42
 */
public class ReadHdfs {
    public static void main(String[] args) throws Exception{
        // 创建 execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stringDataStreamSource = env.readTextFile("hdfs://HDFS80377/test_data/event_log/zsc_new_event_log.lipeng.2019-04-18.log.5");
        stringDataStreamSource.addSink(new KafkaSinkUtil("10.2.40.14:9092,10.2.40.10:9092,10.2.40.15:9092","flink_log3"));
        env.execute("ssss");

    }
}
