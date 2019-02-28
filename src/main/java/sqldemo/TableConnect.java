package sqldemo;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Map;

/**
 * @author wangzhihua
 * @date 2019-02-21 16:11
 */
public class TableConnect {
    public static void main(String[] args) throws Exception {
        System.out.println("===============》 flink任务开始  ==============》");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置检查点时间间隔
        env.enableCheckpointing(5000);
        //设置检查点模式eedeede
        //env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        System.out.println("===============》 开始读取kafka中的数据  ==============》");

        //获取表对象
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        tableEnv.registerTableSource("userTable",new MySource());

        Table table2 = tableEnv.sqlQuery("select userId,behavior from userTable");

        DataStream<Sql_data2redis.info> infoDataStream = tableEnv.toAppendStream(table2, Sql_data2redis.info.class);
        infoDataStream.map(new MapFunction<Sql_data2redis.info, Object>() {

            @Override
            public Object map(Sql_data2redis.info value) throws Exception {
                System.out.println(value.behavior+"8888");
                return "hhh";
            }
        });
        //tableEnv.toRetractStream(table2, info.class);
        //实例化Flink和Redis关联类FlinkJedisPoolConfig，设置Redis端口
        //FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("10.2.40.17").build();
        //实例化RedisSink，并通过flink的addSink的方式将flink计算的结果插入到redis
        //infoDataStream.addSink(new RedisSink<info>(conf, new Sql_data2redis.RedisExampleMapper()));

        System.out.println("===============》 flink任务结束  ==============》");
        env.execute("sql_dara2redis");
    }

    public static class MySource implements StreamTableSource<Row> {

        TypeInformation<Row> typeInfo;
        String[] names = new String[] {"userId" ,"itemId","categoryId","behavior","timestamp"};
        TypeInformation[] types = new TypeInformation[] {Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING()};
        // new RowTypeInfo(types,)

        //类型DataStream必须与TableSource.getReturnType()方法定义的返回类型相同
        @Override
        public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
            DataStreamSource<String> streamSource = execEnv.readTextFile("/Users/apple/Downloads/userData.txt");
            SingleOutputStreamOperator<Row> mapStream = streamSource.map(new MapFunction<String, Row>() {


                @Override
                public Row map(String s) throws Exception {
                    String[] split = s.split(",");
                    Row row =new Row(split.length);
                    for (int i =0;i<split.length;i++){
                        row.setField(i,split[i]);
                    }
                    return row;
                }
            }).returns(new RowTypeInfo(Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING()));
            return mapStream;
        }
       //返回DataStream（StreamTableSource）或DataSet（BatchTableSource）
        @Override
        public TypeInformation<Row> getReturnType() {
           String[] names = new String[] {"userId" ,"itemId","categoryId","behavior","timestamp"};
            TypeInformation[] types = new TypeInformation[] {Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING()};
            typeInfo= Types.ROW(names, types);
            return typeInfo;
        }


       // 返回表的架构
        @Override
        public TableSchema getTableSchema() {
            String[] names = new String[] {"userId" ,"itemId","categoryId","behavior","timestamp"};
            TypeInformation[] types = new TypeInformation[] {Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING()};
            TableSchema tableSchema = new TableSchema(names, types);
            return tableSchema;
        }

        @Override
        public String explainSource() {
            return " ";
        }
    }


}
