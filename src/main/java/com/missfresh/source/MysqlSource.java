package com.missfresh.source;

import java.sql.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DriverManager;


/**
 * @author wangzhihua
 * @date 2019-03-29 14:40
 */
public class MysqlSource  extends RichSourceFunction<String> {
    private  volatile  boolean isRunning =  true ;
    private static final Logger logger = LoggerFactory.getLogger(MysqlSource.class);
    private Connection connection = null;
    private PreparedStatement ps = null;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("com.mysql.jdbc.Driver");//加载数据库驱动
        connection = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/bif?useSSL=false", "root", "518189aA");//获取连接
        ps  = connection.prepareStatement("select job_name from rt_job");

    }
    @Override
    public void run(SourceContext<String> ctx) throws Exception {

        while (isRunning){
            try {
                ResultSet resultSet = ps.executeQuery();
                while (resultSet.next()) {
                    String name = resultSet.getString("job_name");
                    System.out.println(name);
                    ctx.collect(name);//发送结果，结果是tuple2类型，2表示两个元素，可根据实际情况选择
                }
            } catch (Exception e) {
                logger.error("runException:{}", e);
            }
            Thread.sleep(1000);
        }
   }
    @Override
    public void cancel() {
        isRunning=true;
    }
    @Override
    public void close() throws Exception {
        super.close();
          try {
            super.close();
            if (connection != null) {
                connection.close();
            }
            if (ps != null) {
                ps.close();
            }
        } catch (Exception e) {
            logger.error("runException:{}", e);
        }
    }
}