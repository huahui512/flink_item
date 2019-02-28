package com.missfresh.util;


import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @author wangzhihua
 * @date 2019-01-14 10:46
 */
public class Jpools {


    public static Jedis getRedisConn(String url, int  port){
        //创建jedis池配置实例
        JedisPoolConfig config = new JedisPoolConfig();
        JedisPool jedisPool = new JedisPool(config, url, port);
        Jedis jedis = jedisPool.getResource();
        return jedis;
    }


}
