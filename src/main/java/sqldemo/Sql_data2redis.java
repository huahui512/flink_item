package sqldemo;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.table.api.Table;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author wangzhihua
 * @date 2019-01-17 11:33
 */
public class Sql_data2redis {
    public static void main(String[] args) throws Exception {
        String kafkaBrokers = null;
        String zkBrokers = null;
        String topic = null;
        String groupId = null;
        String sqlinfo = null;
        String schema = null;
        if (args.length == 6) {
            kafkaBrokers = args[0];
            zkBrokers = args[1];
            topic = args[2];
            groupId = args[3];
            sqlinfo = args[4];
            schema = args[5];
        } else {
            System.exit(1);
        }
        System.out.println("===============》 flink任务开始  ==============》");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取表对象
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
       //设置任务重启的次数和间隔
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // 尝试重启次数
                Time.of(5, TimeUnit.SECONDS) // 延迟时间间隔
        ));

        //设置kafka连接参数
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaBrokers);
        properties.setProperty("zookeeper.connect", zkBrokers);
        properties.setProperty("group.id", groupId);
        //设置时间类型
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置检查点时间间隔
        env.enableCheckpointing(5000);

        //设置检查点模式
        //env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        System.out.println("===============》 开始读取kafka中的数据  ==============》");
        //创建kafak消费者，获取kafak中的数据
        FlinkKafkaConsumer010<String> kafkaConsumer010 = new FlinkKafkaConsumer010<>(topic, new SimpleStringSchema(), properties);
        kafkaConsumer010.setStartFromEarliest();
        DataStreamSource<String> kafkaData = env.addSource(kafkaConsumer010);
        kafkaData.print();

        //解析kafka数据流 转化成固定格式数据流
        //设置Row的字段名称和类型
        String[] names = new String[] {"behavior","userId","categoryId","itemId","timestamp"};
        TypeInformation[] types = new TypeInformation[] {Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING()};
        RowTypeInfo rowTypeInfo = new RowTypeInfo(types, names);
        //使用Row封装数据类型
        SingleOutputStreamOperator<Row> userData = kafkaData.map(new MapFunction<String, Row>() {
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
        })
                //返回Row封装数据的名称与类型,以便下一个算子能识别此类型
                .returns(rowTypeInfo);

        Table table = tableEnv.fromDataStream(userData);

        tableEnv.registerDataStream("userTable",userData);
        Table table2 = tableEnv.sqlQuery("select behavior ,userId,itemId from userTable");
        DataStream<Row> infoDataStream = tableEnv.toAppendStream(table2, Row.class);
        infoDataStream.map(new MapFunction<Row, Object>() {

            @Override
            public Object map(Row value) throws Exception {
                System.out.println(value.getField(0));

                return "hhh";
            }
        });
        tableEnv.toRetractStream(table2, info.class);
        //实例化Flink和Redis关联类FlinkJedisPoolConfig，设置Redis端口
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("10.2.40.17").setPassword("518189").build();
        //实例化RedisSink，并通过flink的addSink的方式将flink计算的结果插入到redis
        infoDataStream.addSink(new RedisSink<Row>(conf, new Sql_data2redis.RedisExampleMapper()));

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
            return new RedisCommandDescription(RedisCommand.HSET, "pvInfo");
        }

        /**
         * 获取 value值 value的数据是键值对
         * @param data
         * @return
         */
        //指定key
        public String getKeyFromData(Row data) { return data.getField(1).toString(); }
        //指定value
        public String getValueFromData(Row data) {
            return data.getField(0).toString();
        }
    }


    /**        <---  定义一个pojo类要满足一下条件  ----->
     * 1--》这类必须公开。
     * 2--》它必须有一个没有参数的公共构造函数（默认构造函数）。
     * 3--》所有字段都是公共的，或者必须通过getter和setter函数访问。对于一个名为foogetter和setter方法的字段必须命名getFoo()和setFoo()。
     * 4--》Flink必须支持字段的类型。目前，Flink使用Avro序列化任意对象（例如Date）。
     */

    public static class info{
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
                    ", behavior='" + behavior + '\'' +", itemId='" + itemId + '\'' +
                    '}';
        }
    }

}


