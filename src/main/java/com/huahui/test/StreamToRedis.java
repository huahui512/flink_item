package com.huahui.test;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import redis.clients.jedis.Jedis;

public class StreamToRedis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> dss = env.generateSequence(1, 20);

        env.getConfig();

//        auto2redis(dss);

        dss.addSink(new RSink());

        env.execute();
    }

    /**
     * RichSinkFunction实现了SinkFunction，是包含了更多方法的函数（open，close，context）
     * 手动实现flink-redis
     */
    public static class RSink extends RichSinkFunction<Long> {
        Jedis jedis = null;

        //每个task调用一次
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open");
            jedis = new Jedis("localhost", 6380);
            jedis.auth("123456");
        }

        //每个记录调用一次
        @Override
        public void invoke(Long value, Context context) throws Exception {
            System.out.println(value);
            Thread.sleep(2000);
            jedis.set("a", value + "");
        }

        //每个task调用一次
        @Override
        public void close() throws Exception {
            System.out.println("close");
            jedis.close();
        }
    }

    /**
     * 自带的flink-redis
     * @param dss
     */
    private static void auto2redis(DataStreamSource<Long> dss) {
        FlinkJedisPoolConfig jedisBuilder = new FlinkJedisPoolConfig.Builder().setHost("localhost").setPort(6380).setPassword("123456").build();
        dss.addSink(new RedisSink<>(jedisBuilder, new RedisMapper<Long>() {
            //sadd keytest aLong+""
            @Override
            public RedisCommandDescription getCommandDescription() {
                System.out.println("RedisCommandDescription " + jedisBuilder.getMaxIdle() + " " + jedisBuilder.getMaxTotal());
                //要执行的redis命令
                return new RedisCommandDescription(RedisCommand.SADD);
            }

            @Override
            public String getKeyFromData(Long aLong) {
                System.out.println("getKeyFromData" + aLong + " | " + jedisBuilder.getMaxIdle() + " " + jedisBuilder.getMaxTotal());
                //存到redis中的key
                return "keytest";
            }

            @Override
            public String getValueFromData(Long aLong) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("getValueFromData" + aLong);
                //存到redis中的value
                return aLong + "";
            }
        }));
    }
}
