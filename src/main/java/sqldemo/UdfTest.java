package sqldemo;

import com.missfresh.util.GetInfo;
import com.missfresh.util.MyRowInfo;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.concurrent.TimeUnit;

/**
 * @author wangzhihua
 * @date 2019-05-05 11:36
 */
public class UdfTest {
    public static void main(String[] args) throws Exception {
        System.out.println("===============》 flink任务开始  ==============》");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取表对象
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //设置任务重启的次数和间隔
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // 尝试重启次数
                Time.of(5, TimeUnit.SECONDS) // 延迟时间间隔
        ));
        //设置时间类型
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置检查点时间间隔
        env.enableCheckpointing(5000);
        //设置检查点模式

        DataStreamSource<String> streamSource = env.readTextFile("/Users/apple/Downloads/userData.txt");


        String[] names = new String[] {"behavior","userId","categoryId","itemId","timestamp"};
        TypeInformation[] types = new TypeInformation[] {Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING()};
        RowTypeInfo rowTypeInfo = new RowTypeInfo(types, names);
        //使用Row封装数据类型
        SingleOutputStreamOperator<Row> userData = streamSource.map(new MapFunction<String, Row>() {
            //通过循环 获取每一个字段的值
            @Override
            public Row map(String s) throws Exception {
                String[] split = s.split(",");
                Row row = new Row(split.length);
                for (int i =0;i<split.length;i++){
                    row.setField(i,split[i]);
                }
                return row;
            }
        }).returns(rowTypeInfo);

        Table table = tableEnv.fromDataStream(userData);
        tableEnv.registerFunction("stringCode",new UdfDemo1());

        tableEnv.registerDataStream("userTable",userData);
        Table table2 = tableEnv.sqlQuery("select stringCode(userId) ,userId,itemId from userTable");
        DataStream<Row> infoDataStream = tableEnv.toAppendStream(table2, Row.class);
        infoDataStream.map(new MapFunction<Row, Object>() {

            @Override
            public Object map(Row value) throws Exception {
                System.out.println(value.toString());

                return "hhh";
            }
        });


        env.execute("lllll");

    }
}
