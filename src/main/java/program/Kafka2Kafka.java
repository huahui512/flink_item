package program;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @author wangzhihua
 * @date 2019-03-06 16:49
 */
public class Kafka2Kafka {

    public static void main(String[] args) throws Exception {

        System.out.println("===============》 flink任务开始  ==============》");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置时间类型
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置检查点时间间隔
        env.enableCheckpointing(5000);
        //设置检查点模式
        //env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        System.out.println("===============》 开始读取kafka中的数据  ==============》");
        DataStreamSource<String> streamSource = env.socketTextStream("127.0.0.1", 9000);
        streamSource.addSink(new KafkaSinkUtil("127.0.0.1:9092","test"));
        env.execute("2.3");

        }

    }


































