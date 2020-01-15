package program;

import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * @author wangzhihua
 * @date 2019-01-11 12:03
 */
public class AsynsRedisRead {
    public static void main(String[] args) throws Exception {
        System.out.println("===============》 flink任务开始  ==============》");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        System.out.println("===============》 开始读取kafka中的数据  ==============》");
        SingleOutputStreamOperator<String> kafkaData = env.readTextFile("/Users/apple/app/1m.txt");
        SingleOutputStreamOperator<String> unorderedWait = AsyncDataStream.unorderedWait(kafkaData, new AsyncReadRedis(), 1000, TimeUnit.MICROSECONDS, 4);
        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String time= sdf.format( new Date());
        System.out.println(time+"start");
        unorderedWait.process(new ProcessFunction<String, Object>() {
           int num =0;
           @Override
           public void processElement(String value, Context ctx, Collector<Object> out) throws Exception {
               num++;
               System.out.println(num+"====");
               if(num==9999){
                   String time= sdf.format( new Date());
                   System.out.println(time+"end");
                   System.exit(0);
               }
           }
       }).setParallelism(4);
        //设置程序名称
        env.execute("data_");
    }
}