package program;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.*;

/**
 * @author wangzhihua
 * @date 2019-06-26 16:55
 */
public class KafkaJavaApi {
    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        // 定义kakfa 服务的地址，不需要将所有broker指定上
        props.put("bootstrap.servers", "10.2.40.14:9092,10.2.40.10:9092,10.2.40.15:9092");
        // 制定consumer group
        props.put("group.id", "2f2d3");
        // 是否自动确认offset
        props.put("enable.auto.commit", "true");
        // 自动确认offset的时间间隔
        props.put("auto.commit.interval.ms", "1000");
        // props.put("auto.offset.reset", "earliest");
        props.put("max.poll.records", "1");
        // key的序列化类
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // value的序列化类
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 定义consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        // 消费者订阅的topic, 可同时订阅多个
        consumer.subscribe(Arrays.asList("flink_log3"));
        String str = null;
        boolean b=true;
        while (b) {
            // 读取数据，读取超时时间为100ms
            ConsumerRecords<String, String> records = consumer.poll(500);
            if(records.count()>0){
                int count = 0;
                for (ConsumerRecord<String, String> record : records) {
                    count++;
                    System.out.println(record.toString());
                    str = record.value();
                    b=false;
                    if (str != null) {
                        System.out.println(count);
                        break;
                    }
                }
            }
        }
        System.out.println(str);

    }

}