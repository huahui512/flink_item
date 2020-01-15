package com.huahui.sqldemo;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @author wangzhihua
 * @date 2019-05-14 14:50
 */
public class JoinTable {
   public static DataStream<String>  RegisterJoinTable(DataStreamSource<String> streamSource, StreamTableEnvironment tableEnv,JoinFlatFun joinFlatFun,String scama){
      DataStream<String> result = streamSource.flatMap(joinFlatFun);
       String[] names = new String[] {"behavior","userId","categoryId","itemId","timestamp"};
       TypeInformation[] types = new TypeInformation[] {Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING()};
       RowTypeInfo rowTypeInfo = new RowTypeInfo(types, names);
      // result

       /*tableEnv.registerDataStream("t1",result);*/
       return result;
   }
}
