package program;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import redis.clients.jedis.*;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * @author wangzhihua
 * @date 2019-04-02 11:17
 */
public class AsyncReadRedis extends RichAsyncFunction<String,String> {
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
    //private static int TIMEOUT = 100000;
    Pipeline pipelined=null;
    int count=0;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        /*config = new JedisPoolConfig();
        jedisPool = new JedisPool(config, ADDR, PORT, TIMEOUT, AUTH, 5);
        jedis = jedisPool.getResource();*/

            jedis = new Jedis(ADDR, PORT);
            jedis.auth(AUTH);
            jedis.select(5);

    }
    //数据异步调用
    @Override
    public void asyncInvoke(String input, ResultFuture<String> resultFuture) throws Exception {
        // 发起一个异步请求，返回结果的future
        CompletableFuture<String> completableFuture = CompletableFuture.supplyAsync(new Supplier<String>() {
            @Override
            public String get() {
                try {
                    String[] split = input.split(",");
                    String name = split[1];
                    String s = jedis.hget("AsyncReadRedis", name);
                    return split[0]+"==>"+s;
                } catch (Exception e) {
                    e.printStackTrace();
                    return null;
                }
            }
        });
        //获取异步请求的结果，传递给下游
        completableFuture.thenAccept((String dbResult) -> {
            // 设置请求完成时的回调: 将结果传递给 collector
            resultFuture.complete(Collections.singleton(dbResult));
        });
    }
    @Override
    public void timeout(String input, ResultFuture resultFuture) throws Exception {
    }
    @Override
    public void close() throws Exception {
        super.close();
    }

}
