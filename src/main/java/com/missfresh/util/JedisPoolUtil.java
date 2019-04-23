package com.missfresh.util;


import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @author wangzhihua
 * @date 2019-01-14 10:46
 */
public class JedisPoolUtil {
    // 初始化一个空的jedisPool
    private static volatile JedisPool jedisPool;
    private static volatile JedisPoolUtil jedisPoolUtil;
    // 获取redisIp
    private static final String REDISIP = "10.2.40.17";
    // 获取redisPort
    private static final Integer REDISPORT = 6379;
    // 获取redis连接超时时间
    private static final Integer REDISTIMEOUT = 30000;
    // 获取redis密码
    private static final String REDISPASSWORD = "518189aA";
    // 获取redis最大连接数
    private static final Integer REDISMAXACTIVE = 50;
    // 获取redis最大空闲连接数
    private static final Integer MAXIDLE = 50;
    // jedis池没有连接对象返回时，等待可用连接的最大时间，单位毫秒，默认值为-1，表示永不超时。
    private static final Long MAXWAIT = -1L;
    // 从池中获取连接的时候，是否进行有效检查
    private static final boolean TESTONBORROW = true;
    // 归还连接的时候，是否进行有效检查
    private static final boolean TESTONRETURN = true;
    // 不允许通过new创建该类的实例
    private JedisPoolUtil() {
        initialPool();
    }

    /**
     * 初始化Redis连接池
     */
    public  void initialPool() {
        try {
            // 创建jedis池配置实例
            JedisPoolConfig config = new JedisPoolConfig();
            // 设置池配置项值
            config.setMaxTotal(REDISMAXACTIVE);
            config.setMaxIdle(MAXIDLE);
            config.setMaxWaitMillis(MAXWAIT);
            config.setTestOnBorrow(TESTONBORROW);
            config.setTestOnReturn(TESTONRETURN);
            // 根据配置实例化jedis池
            jedisPool = new JedisPool(config, REDISIP,
                    REDISPORT,
                    REDISTIMEOUT,
                    REDISPASSWORD
            );
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 双重试单例获取连接
     *
     * @return Jedis
     */
    public static JedisPoolUtil getInstance() {
        if (jedisPoolUtil == null) {
            synchronized (JedisPoolUtil.class) {
                if (jedisPoolUtil == null) {
                    jedisPoolUtil = new JedisPoolUtil();
                }
            }
        }
        return jedisPoolUtil;
    }

    public Jedis getJedis() {
        return jedisPool.getResource();
    }

    /**
     * 释放jedis资源
     *
     * @param jedis
     */
    public void returnResource(Jedis jedis) {
        if (jedis != null && jedisPool != null) {
            jedis.close();
        }
    }

    /**
     * 关闭池子
     */
    public void closePool() {
        if (jedisPool != null) {
            jedisPool.close();
        }
    }


}
