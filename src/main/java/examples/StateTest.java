package examples;

import com.missfresh.source.MysqlSource;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @author wangzhihua
 * @date 2019-05-16 16:02
 */
public class StateTest {

    public static void main(String[] args) throws Exception {
        System.out.println("===============》 flink任务开始  ==============》");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> mysqlData  = env.addSource(new MysqlSource());
        DataStreamSource<String> streamSource = env.socketTextStream("127.0.0.1",3233);

        streamSource.map(new RichMapFunction<String, Object>() {
            ValueState<Integer> state;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>("count", Integer.class, 0);
               state= getRuntimeContext().getState(descriptor);
            }

            @Override
            public Tuple2 map(String value) throws Exception {
                Integer value1 = state.value();
                value1++;
                state.update(value1);
                Tuple2 tuple2 = new Tuple2(value, value1);
                return tuple2;
            }
        }).print();


        env.execute("sss");
    }
}
