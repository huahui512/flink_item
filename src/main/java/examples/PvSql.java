package examples;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author wangzhihua
 * @date 2018-12-27 10:50
 */
public class PvSql {
    public static void main(String[] args) {
        // 创建 execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 告诉系统按照 EventTime 处理
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 为了打印到控制台的结果不乱序，我们配置全局的并发为1，改变并发对结果正确性没有影响
        env.setParallelism(1);
        //读取hdfs中的测试数据
        DataStream<String> source = env.readTextFile("hdfs://HDFS80377/test/user.txt");

    }
}
