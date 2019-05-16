package sqldemo;


import com.missfresh.util.GetInfo;
import com.missfresh.util.MyRowInfo;
import com.missfresh.util.Parameter;
import com.missfresh.util.SqlParse;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
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
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentFactory;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author wangzhihua
 * @date 2019-02-20 16:47
 */
public class LoaclSqlTest {
    public static void main(String[] args) throws Exception {
        Map<String,String> parMap = Parameter.getParm(args);
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
        //env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        System.out.println("===============》 开始读取kafka中的数据  ==============》");
        //获取表的字段名和类型
        String typeInfo = GetInfo.getTypeInfo();
        //设置Row的字段名称和类型
        RowTypeInfo rowTypeInfo = MyRowInfo.getRowTypeInfo(typeInfo);
        DataStreamSource<String> streamSource = env.readTextFile("/Users/apple/Downloads/userData.txt");
        //TypeInformation[] rowType = MyRowInfo.getRowType(typeInfo);
        TypeInformation[] typeInformations = rowTypeInfo.getFieldTypes();
        //使用Row封装数据类型
        SingleOutputStreamOperator<Row> userData = streamSource.map(new MapFunction<String, Row>() {
            //通过循环 获取每一个字段的值
            @Override
            public Row map(String s) throws Exception {
                Row row = null;
                try {
                    String[] split = s.split(",");
                    row = new Row(split.length);
                    for (int i = 0; i < split.length; i++) {
                        String typeStr = typeInformations[i].toString();
                        if ("Integer".equals(typeStr)) {
                            row.setField(i, Integer.valueOf(split[i]));
                        } else if ("Long".equals(typeStr)) {
                            row.setField(i, Long.parseLong(split[i]));
                        } else if ("Double".equals(typeStr)) {
                            row.setField(i, Double.parseDouble(split[i]));
                        } else if ("Float".equals(typeStr)) {
                            row.setField(i, Float.parseFloat(split[i]));
                        } else {
                            row.setField(i, String.valueOf(split[i]));
                        }
                    }
                } catch (NumberFormatException e) {
                    e.printStackTrace();
                }
                return row;

            }
        })
                //返回Row封装数据的名称与类型,以便下一个算子能识别此类型
                .returns(rowTypeInfo);

        Table table = tableEnv.fromDataStream(userData);

        tableEnv.registerDataStream("userTable", userData);
        Table table2 = tableEnv.sqlQuery("select count(distinct userId) as uv ,behavior from userTable group by behavior");

        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tableEnv.toRetractStream(table2, Row.class);

        SingleOutputStreamOperator<Row> outputStream = tuple2DataStream.map(new MapFunction<Tuple2<Boolean, Row>, Row>() {
            @Override
            public Row map(Tuple2<Boolean, Row> value) throws Exception {
                Row row1 = value.f1;
                System.out.println(row1.toString());
                return row1;
            }
        });

       // outputStream.addSink(new EsOutPut("select count(distinct userId) as uv ,behavior from userTable group by behavior"));
        System.out.println("===============》 flink任务结束  ==============》");
        env.execute("sql_dara2redis");
    }


    /**
     * 数据写入redis
     */
    //指定Redis key并将flink数据类型映射到Redis数据类型
    public static final class RedisExampleMapper implements RedisMapper<Row> {
        //设置数据使用的数据结构 HashSet 并设置key的名称
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "PVData");
        }

        /**
         * 获取 value值 value的数据是键值对
         *
         * @param data
         * @return
         */
        //指定key
        public String getKeyFromData(Row data) {
            return data.getField(1).toString();
        }

        //指定value
        public String getValueFromData(Row data) {
            return data.getField(0).toString();
        }
    }


    /**
     * <---  定义一个pojo类要满足一下条件  ----->
     * 1--》这类必须公开。
     * 2--》它必须有一个没有参数的公共构造函数（默认构造函数）。
     * 3--》所有字段都是公共的，或者必须通过getter和setter函数访问。对于一个名为foogetter和setter方法的字段必须命名getFoo()和setFoo()。
     * 4--》Flink必须支持字段的类型。目前，Flink使用Avro序列化任意对象（例如Date）。
     */

    public static class info {
        public String userId;
        public String behavior;
        public String itemId;

        public info() {
        }

        public info(String userId, String behavior, String itemId) {
            this.userId = userId;
            this.behavior = behavior;
            this.itemId = itemId;
        }

        @Override
        public String toString() {
            return "info{" +
                    "userId='" + userId + '\'' +
                    ", behavior='" + behavior + '\'' + ", itemId='" + itemId + '\'' +
                    '}';
        }
    }

}