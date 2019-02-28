package examples;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
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

import java.util.Properties;

/**
 * @author wangzhihua
 * @date 2019-01-11 12:03
 */
public class Data2Redis {
    public static void main(String[] args) throws Exception {
        String kafkaBrokers = null;
        String zkBrokers = null;
        String topic = null;
        String groupId = null;
        if (args.length == 4) {
            kafkaBrokers = args[0];
            zkBrokers = args[1];
            topic = args[2];
            groupId = args[3];
        } else {
            System.exit(1);
        }
        System.out.println("===============》 flink任务开始  ==============》");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        System.out.println("===============》 开始读取kafka中的数据  ==============》");
        //创建kafak消费者，获取kafak中的数据
        FlinkKafkaConsumer010<String> kafkaConsumer010 = new FlinkKafkaConsumer010<>(topic, new SimpleStringSchema(), properties);
        kafkaConsumer010.setStartFromEarliest();
        SingleOutputStreamOperator<String> kafkaData = env.addSource(kafkaConsumer010).uid("source1");
        kafkaData.print();
        //解析kafka数据流 转化成固定格式数据流
        SingleOutputStreamOperator<Tuple5<Long, Long, Long, String, Long>>  userData = kafkaData.map(new MapFunction<String, Tuple5<Long, Long, Long, String, Long>>() {
            @Override
            public Tuple5<Long, Long, Long, String, Long> map(String s) throws Exception {
                String[] split = s.split(",");
                long userID = Long.parseLong(split[0]);
                long itemId = Long.parseLong(split[1]);
                long categoryId = Long.parseLong(split[2]);
                String behavior = split[3];
                long timestamp = Long.parseLong(split[4]);
                Tuple5<Long, Long, Long, String, Long> userInfo = new Tuple5<>(userID, itemId, categoryId, behavior, timestamp);
                return userInfo;
            }
        }).uid("map1");
        //实例化Flink和Redis关联类FlinkJedisPoolConfig，设置Redis端口
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("10.2.40.17").build();
        //实例化RedisSink，并通过flink的addSink的方式将flink计算的结果插入到redis
        userData.addSink(new RedisSink<Tuple5<Long, Long, Long, String, Long>>(conf, new RedisExampleMapper()));

        System.out.println("===============》 flink任务结束  ==============》");

        //设置程序名称
        env.execute("data_to_redis_wangzh");
    }

    //指定Redis key并将flink数据类型映射到Redis数据类型
    public static final class RedisExampleMapper implements RedisMapper<Tuple5<Long, Long, Long, String, Long>> {
        //设置数据使用的数据结构 HashSet 并设置key的名称
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "flink");
        }

        /**
         * 获取 value值 value的数据是键值对
         * @param data
         * @return
         */
        //指定key
        @Override
        public String getKeyFromData(Tuple5<Long, Long, Long, String, Long> data) {
            return data.f0.toString();
        }
        //指定value
        @Override
        public String getValueFromData(Tuple5<Long, Long, Long, String, Long> data) {
            return data.f1.toString()+data.f3;
        }
    }
}