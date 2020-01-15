package com.huahui.source;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collections;

/**
 * @author wangzhihua
 * @date 2019-03-29 14:40
 */
public  class MysqlSource2 extends RichAsyncFunction<String,String> {

    private  volatile  boolean isRunning =  true ;
    private static final Logger logger = LoggerFactory.getLogger(MysqlSource.class);
    private Connection connection = null;
    private PreparedStatement ps = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("com.mysql.jdbc.Driver");//加载数据库驱动
        connection = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/bifrost?useSSL=false", "root", "518189aA");//获取连接
        ps  = connection.prepareStatement("select * from userinfo");
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void asyncInvoke(String input, ResultFuture resultFuture) throws Exception {
        ResultSet resultSet = ps.executeQuery();
        String str=null;
        String userId=null;
        String city=null;
        String u_time=null;

        while (resultSet.next()) {
           userId = resultSet.getString("userId");
            if (input.equals(userId)) {
                 city = resultSet.getString("city");
                 u_time= resultSet.getString("utime");
                str=userId+","+city+","+u_time;
            }else {
                str="-";
            }


            resultFuture.complete(Collections.singleton(new Tuple2<String ,String>(input,str)));
        }


    }


}
