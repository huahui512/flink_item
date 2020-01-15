package program;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author wangzhihua
 * @date 2019-01-11 12:03
 */
public class UnAsynsRedisRead {
    public static void main(String[] args) throws Exception {
        System.out.println("===============》 flink任务开始  ==============》");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        System.out.println("===============》 开始读取kafka中的数据  ==============》");
        SingleOutputStreamOperator<String> kafkaData = env.readTextFile("/Users/apple/app/50m.txt");
        long l1 = System.currentTimeMillis();
        SingleOutputStreamOperator<String> unorderedWait = kafkaData.map(new redisMap());
        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String time= sdf.format( new Date());
        System.out.println(time+"start");
       unorderedWait.process(new ProcessFunction<String, Object>() {
            int num =0;
            @Override
            public void processElement(String value, Context ctx, Collector<Object> out) throws Exception {
               // System.out.println(num);
                num++;
                if(num==20000){
                    String time= sdf.format( new Date());
                    System.out.println(time+"end");
                    System.exit(0);
                }
            }
        }).setParallelism(1);
        //设置程序名称
        env.execute("data_");
    }
}