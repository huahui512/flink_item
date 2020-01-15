package com.huahui.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

/**
 * @author wangzhihua
 * @date 2019-03-21 15:33
 */
public class RedisSource {
    public static void main(String[] args) {
        ArrayList<String> strings = new ArrayList<>();
        strings.add("a");
        strings.add("a");
        for (String s : strings) {
            System.out.println(s);
        }
    }


    public static class MyRedisSource extends RichSourceFunction<String> {
        //获取连接池的配置对象
        private JedisPoolConfig config = null;
        //获取连接池
        JedisPool jedisPool = null;
        //获取核心对象
        Jedis jedis = null;
        //Redis服务器IP
        private static String ADDR = "10.2.40.17";
        //Redis的端口号
        private static int PORT = 6379;
        //访问密码
        private static String AUTH = "518189aA";
        //等待可用连接的最大时间，单位毫秒，默认值为-1，表示永不超时。如果超过等待时间，则直接抛出JedisConnectionException；
        private static int TIMEOUT = 10000;
        private volatile boolean isRunning = true;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            config = new JedisPoolConfig();
            jedisPool = new JedisPool(config, ADDR, PORT, TIMEOUT, AUTH, 5);
            jedis = jedisPool.getResource();


        }

        /**
         * 启动一个 source，即对接一个外部数据源然后 emit 元素形成 stream
         * （大部分情况下会通过在该方法里运行一个 while 循环的形式来产生 stream）。
         *
         * @param ctx
         * @throws Exception
         */
        @Override
        public void run(SourceContext<String> ctx) throws Exception {

            while (isRunning) {
                Map<String, String> map = jedis.hgetAll("joint");
                Set<Map.Entry<String, String>> entries = map.entrySet();
                for (Map.Entry<String, String> entry : entries) {
                    ctx.collect(entry.getKey() + "," + entry.getValue());
                }
                Thread.sleep(5000);
            }
        }

        /**
         * 取消源。大多数源文件都有一个while循环
         * {@link #run(SourceContext)}方法。实现需要确保
         * 调用此方法后，source将跳出该循环。
         */

        @Override
        public void cancel() {
            isRunning = false;
        }

        /**
         * 它是在最后一次调用主工作方法之后调用的, 此方法可用于清理工作
         */
        @Override
        public void close() throws Exception {
            super.close();
            jedisPool.close();
            jedis.close();

        }
    }





}


