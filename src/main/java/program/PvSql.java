package program;

import com.huahui.source.MysqlSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;

/**
 * @author wangzhihua
 * @date 2018-12-27 10:50
 */
public class PvSql {
    public static void main(String[] args) throws Exception {
        // 创建 execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 告诉系统按照 EventTime 处理
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 为了打印到控制台的结果不乱序，我们配置全局的并发为1，改变并发对结果正确性没有影响
        DataStreamSource<String> source1= env.addSource(new MysqlSource());

       // DataStreamSource<String> source2= env.addSource(new RedisSource.MyRedisSource());
        HashMap<String, String > map1 = new HashMap<>();

        source1.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                System.out.println(value);
               map1.put(value,value);
                System.out.println(map1.get("ee")+"========");
                return " ";
            }
        });



            System.out.println(map1.get("ee"));

        /*source2.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                System.out.println("liu2"+value);
                return " ";
            }
        });*/

        env.execute("ssss");

    }
}
