package program;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;

/**
 * @author wangzhihua
 * @date 2019-03-27 10:42
 */
public class WindowsTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
      ///  SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        System.out.println("============》 任务开始+++++");
        //kafkaConsumer1.setStartFromEarliest();

        DataStreamSource<String> source1 = env.socketTextStream("127.0.0.1",6666);
        source1.print();

        SingleOutputStreamOperator<info1> stream1 = source1.map(new MapFunction<String, info1>() {
            @Override
            public info1 map(String value) throws Exception {
                info1 info2 = new info1();
                    String[] split = value.split(",");
                    if (split.length==3) {
                        String timeStamp = split[0];
                        String name = split[1];
                        String count = split[2];

                        info2.setTimeStamp(timeStamp);
                        info2.setItemId(name);
                        info2.setCount(Integer.parseInt(count));

                    }

                return info2;
            }
        }).uid("ee").name("rrr").setParallelism(1);


        SingleOutputStreamOperator<info1> stream1AndWatermarks = stream1
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<info1>() {
              long currentMaxTimestamp = 0L;
              long maxOutOfOrderness = 3000L;
              Watermark watermark = null;
              //最大允许的乱序时间是10s
              @Nullable
              @Override
              public Watermark getCurrentWatermark() {
                  watermark = new Watermark(currentMaxTimestamp - maxOutOfOrderness);
                  return watermark;
              }
              @Override
              public long extractTimestamp(info1 element, long previousElementTimestamp) {
                long timeStamp=Long.parseLong(element.getTimeStamp());
                  currentMaxTimestamp = Math.max(timeStamp, currentMaxTimestamp);
                  return timeStamp;
              }
          }
        );

        SingleOutputStreamOperator<info1> count = stream1AndWatermarks
                .keyBy("itemId")
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(2))
                .sum("count");
        count.map(new MapFunction<info1, Object>() {
            @Override
            public Object map(info1 value) throws Exception {
                System.out.println(value.toString());
                return " ";
            }
        });

        try {
            env.execute("DimensionTablejoin");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    public static class info1 {
        public String timeStamp;
        public String itemId;
        int count;

        public info1(String timeStamp, String itemId, int count) {
            this.timeStamp = timeStamp;
            this.itemId = itemId;
            this.count = count;
        }

        public info1() {
        }

        public String getTimeStamp() {
            return timeStamp;
        }

        public void setTimeStamp(String timeStamp) {
            this.timeStamp = timeStamp;
        }

        public String getItemId() {
            return itemId;
        }

        public void setItemId(String itemId) {
            this.itemId = itemId;
        }

        public int getCount() {
            return count;
        }

        @Override
        public String toString() {
            return "info1{" +
                    "timeStamp='" + timeStamp + '\'' +
                    ", itemId='" + itemId + '\'' +
                    ", count=" + count +
                    '}';
        }

        public void setCount(int count) {
            this.count = count;
        }
    }


}
