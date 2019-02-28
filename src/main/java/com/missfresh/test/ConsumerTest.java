package com.missfresh.test;

import com.missfresh.test.util.JdbcUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Properties;

/**
 * @author wangtaiyang
 */
public class ConsumerTest {
    public static void main(String[] args) throws InterruptedException {
        Character separator = '\u0000';
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "abc1");
        props.put("enable.auto.commit", "false");
//        props.put("auto.offset.reset", "earliest");
        props.put("auto.offset.reset", "latest");

        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(Collections.singletonList("grampus_order_etl"));
        ConsumerRecords<String, String> records = null;
        while (true) {
            records = kafkaConsumer.poll(500);
            if (records.count() == 0) {
                Thread.sleep(1000);
            } else {
                break;
            }
        }
        Connection conn = JdbcUtil.getConn();
        PreparedStatement statement = null;
        try {
            statement = conn.prepareStatement("insert into gms_order(id,store_code,order_id,pay_price,total_price,discount_price," +
                    "pay_type,order_status,user_id,timeout,user_coupon_id,goods_coupon_id,status,modify_time,create_time,order_type,wx_activity_id,enterprise_price," +
                    "ext_info,order_source)values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");

            int count = 0;
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.toString());
                String[] split = record.value().split(separator.toString());
                System.out.println("字段数量：" + split.length);
                for (int j = 0; j < split.length; j++) {
                    String s = split[j];
                    statement.setObject(j + 1, s);
                }
                statement.execute();
                count++;
                if (count > 2) {
                    System.exit(0);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                assert statement != null;
                statement.close();
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        System.out.println("插入成功");
        kafkaConsumer.close();
    }
}
