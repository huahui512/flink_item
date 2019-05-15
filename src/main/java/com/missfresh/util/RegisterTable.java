package com.missfresh.util;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.types.Row;

/**
 * @author wangzhihua
 * @date 2019-05-15 16:22
 */
public class RegisterTable {


    public  static  void  getTable(String streamT,String dimT,StreamTableEnvironment tableEnv , DataStreamSource<Row> userData, DataStreamSource<Row> userInfo){
        //注册表并设置字段名和时间戳类型
        Table table = tableEnv.fromDataStream(userData,"userId,behavior,categoryId,itemId,ctime,proctime.proctime");
        Table table2 = tableEnv.fromDataStream(userInfo,"userId,city,utime,proctime.proctime");
        tableEnv.registerTable(streamT,table);
        tableEnv.registerTable(streamT+"0",table2);
        //生成临时表函数，设置timeAttribute类型和维表的主键
        TemporalTableFunction t3 = table2.createTemporalTableFunction("proctime","userId");
        tableEnv.registerFunction(streamT,t3);
    }
}
