package com.huahui.test;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author wangtaiyang
 */
public class BatchTest {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        List<Integer> list = new ArrayList<Integer>();
        list.add(1);
        list.add(2);
        list.add(3);
        list.add(4);
        list.add(5);
        DataSource<Integer> ds = env.fromCollection(list);
        ds.print();

        ds.map(value -> {
            Jedis jedis = new Jedis("localhost", 6380);
            jedis.auth("123456");
            System.out.println(value + "##");
            String a = jedis.get("a");
            System.out.println("redis a: " + a);
            jedis.set("a", "abc");
            return value + "##";
        }).output(new OutputFormat<String>() {
            //            Jedis jedis = new Jedis("localhost", 6379);
            @Override
            public void configure(Configuration parameters) {
                System.out.println("configure");
            }

            @Override
            public void open(int taskNumber, int numTasks) throws IOException {
                System.out.println("open");
            }

            @Override
            public void writeRecord(String record) throws IOException {

                System.out.println("writeRecord");
            }

            @Override
            public void close() throws IOException {
                System.out.println("close");
            }
        });

        env.execute();
    }
}
