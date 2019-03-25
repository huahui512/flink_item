package examples;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author wangzhihua
 * @date 2019-03-18 16:43
 */
public class test {
    public static void main(String[] args) throws Exception {
        System.out.println("===============》 flink任务开始  ==============》");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.fromElements("ttttttttttt");
        source.print();

        System.out.println("111111111111111111111111");
        //获取表对象
        env.execute("jjjj");
    }
}
