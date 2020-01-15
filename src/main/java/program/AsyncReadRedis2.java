package program;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.redisson.Redisson;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * @author wangzhihua
 * @date 2019-04-02 11:17
 */
public class AsyncReadRedis2 extends RichAsyncFunction<String,String> {

    Config config=null;
    //访问密码
    private static String AUTH = "518189aA";
    RedissonClient redisson=null;
    RMap<String, String> rMap=null;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        config=new Config();
        config.setCodec(new org.redisson.client.codec.StringCodec());

        //指定使用单节点部署方式
        config.useSingleServer().setAddress("redis://10.2.40.17:6379");
        config.useSingleServer().setDatabase(5);//设置库
        config.useSingleServer().setPassword(AUTH);
        config.useSingleServer().setConnectionPoolSize(500);
        config.useSingleServer().setIdleConnectionTimeout(10000);
        config.useSingleServer().setConnectTimeout(30000);
        config.useSingleServer().setTimeout(3000);
        config.useSingleServer().setPingTimeout(30000);
        config.useSingleServer().setReconnectionTimeout(3000);
        //创建客户端(发现这一非常耗时，基本在2秒-4秒左右)
        redisson = Redisson.create(config);
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
                    rMap = redisson.getMap("AsyncReadRedis");
                    String s =rMap.get(name);
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


    public static void main(String[] args) {
        Config config=null;
        String ADDR = "10.2.40.17";
        RedissonClient redisson=null;
        config=new Config();
        config.setCodec(new org.redisson.client.codec.StringCodec());

        //指定使用单节点部署方式
        config.useSingleServer().setAddress("redis://10.2.40.17:6379");
        config.useSingleServer().setDatabase(5);//设置库
        config.useSingleServer().setPassword(AUTH);
        config.useSingleServer().setConnectionPoolSize(500);//设置对于master节点的连接池中连接数最大为500
        config.useSingleServer().setIdleConnectionTimeout(10000);//如果当前连接池里的连接数量超过了最小空闲连接数，而同时有连接空闲时间超过了该数值，那么这些连接将会自动被关闭，并从连接池里去掉。时间单位是毫秒。
        config.useSingleServer().setConnectTimeout(30000);//同任何节点建立连接时的等待超时。时间单位是毫秒。
        config.useSingleServer().setTimeout(3000);//等待节点回复命令的时间。该时间从命令发送成功时开始计时。
        config.useSingleServer().setPingTimeout(30000);
        config.useSingleServer().setReconnectionTimeout(3000);//当与某个节点的连接断开时，等待与其重新建立连接的时间间隔。时间单位是毫秒。
        //创建客户端(发现这一非常耗时，基本在2秒-4秒左右)
        redisson = Redisson.create(config);
        RMap<String, String> rMap = redisson.getMap("AsyncReadRedis");
        String s =rMap.get("beijing");
        System.out.println(s);
    }

}
