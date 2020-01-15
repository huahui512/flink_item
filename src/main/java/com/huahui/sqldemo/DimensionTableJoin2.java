package com.huahui.sqldemo;

import com.huahui.source.MysqlSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.types.Row;

import java.sql.Timestamp;

/**
 * @author wangzhihua
 * @date 2019-05-07 14:22
 */
public class DimensionTableJoin2 {
    public static void main(String[] args) throws Exception {
        System.out.println("===============》 flink任务开始  ==============》");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String>  mysqlData  = env.addSource(new MysqlSource());
        mysqlData.print();
        //获取表对象
        EnvironmentSettings setting = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,setting);//设置时间类型
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置检查点时间间隔
        env.enableCheckpointing(5000);
        //获取实时流
        DataStreamSource<String> streamSource = env.socketTextStream("127.0.0.1",3233);
        //设置数据类型和名称
        String[] names = new String[] {"userId","behavior","categoryId","itemId","ctime"};
        String[] names2 = new String[] {"userId","city","utime"};
        TypeInformation[] types = new TypeInformation[] {Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),Types.SQL_TIMESTAMP()};
        TypeInformation[] types2 = new TypeInformation[] {Types.STRING(), Types.STRING(),Types.SQL_TIMESTAMP()};
        RowTypeInfo rowTypeInfo = new RowTypeInfo(types, names);
        RowTypeInfo rowTypeInfo2 = new RowTypeInfo(types2, names2);

        //使用Row封装数据类型，获取实时流
        SingleOutputStreamOperator<Row> userData = streamSource.map(new MapFunction<String, Row>() {
            //通过循环 获取每一个字段的值
            @Override
            public Row map(String s) throws Exception {
                String[] split = s.split(",");
                Row row = new Row(split.length);
                for (int i =0;i<split.length;i++){
                    if(i==split.length-1){
                        row.setField(i, Timestamp.valueOf(split[i]));
                    }else{
                        row.setField(i, split[i]);
                  }
                }
                return row;
            }
        }).returns(rowTypeInfo);
        userData.print();
        //获取维表数据
        DataStream<Row> userInfo = mysqlData.map(new MapFunction<String, Row>() {
            @Override
            public Row map(String s) throws Exception {
                String[] split = s.split(",");
                Row row = new Row(split.length);
                for (int i = 0; i < split.length; i++) {
                    if(i==split.length-1){
                        row.setField(i, Timestamp.valueOf(split[i]));
                    }else{
                        row.setField(i, split[i]);
                    }
                }
                return row;
            }
        }).returns(rowTypeInfo2);

        userInfo.print();

        //注册表并设置字段名和时间戳类型
        Table table = tableEnv.fromDataStream(userData,"userId,behavior,categoryId,itemId,ctime,proctime.proctime");
        Table table2 = tableEnv.fromDataStream(userInfo,"userId,city,utime,proctime.proctime");
        tableEnv.registerTable("T1",table);
        tableEnv.registerTable("T2",table2);
        //生成临时表函数，设置timeAttribute类型和维表的主键
        TemporalTableFunction t3 = table2.createTemporalTableFunction("proctime","userId");
        tableEnv.registerFunction("T3",t3);
        //临时表join语句
        String sqlinfo2="select *  from T1 JOIN  LATERAL TABLE (T3(proctime)) AS  p ON T1.userId = p.userId   where p.userId='561558' ";
        //返回结果表
        Table table3 = tableEnv.sqlQuery(sqlinfo2);
        DataStream<Tuple2<Boolean, Row>> joinResult = tableEnv.toRetractStream(table3, Row.class);
        joinResult.print();
        env.execute("ddddddd");
    }



}
