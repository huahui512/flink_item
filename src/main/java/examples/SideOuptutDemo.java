package examples;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author wangzhihua
 * @date 2019-04-28 17:48
 */
public class SideOuptutDemo {
    private static final OutputTag<String> rejectedWordsTag = new OutputTag<String>("rejected") {};
    public static void main(String[] args) throws Exception {

        // 创建 execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.readTextFile("/Users/apple/Desktop/word.txt");
        SingleOutputStreamOperator<String> processData= source.process(new Tokenizer());
        SingleOutputStreamOperator<String> rejectWord = processData.getSideOutput(rejectedWordsTag)
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String value) throws Exception {
                        return "长度大于5的单词" + value;
                    }
                });
        processData.print();
        rejectWord.print();


        env.execute("SideOuptut");
    }

    /**
     * 以用户自定义FlatMapFunction函数的形式来实现分词器功能，该分词器会将分词封装为(word,1)，
     * 同时不接受单词长度大于5的，也即是侧输出都是单词长度大于5的单词。
     */
    public static final class Tokenizer extends ProcessFunction<String, String> {
        private static final long serialVersionUID = 1L;

        @Override
        public void processElement(
                String value,
                Context ctx,
                Collector<String> out) throws Exception {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split(",");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 5) {
                    ctx.output(rejectedWordsTag, token);
                } else {
                    out.collect(token);
                }
            }

        }

}


}