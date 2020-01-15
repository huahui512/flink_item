package com.huahui.sqldemo;

import com.huahui.source.MysqlSource;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

/**
 * @author wangzhihua
 * @date 2019-05-14 14:53
 */
public  class  JoinFlatFun extends RichFlatMapFunction<String,String> {
    private static final Logger logger = LoggerFactory.getLogger(MysqlSource.class);
    private Connection connection = null;
    private String  tname;

    public JoinFlatFun(String tname) {
        this.tname = tname;
    }

    private PreparedStatement ps = null;
    Map<String,String> map=new HashMap();

    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        TimerTask timerTask = new TimerTask() {
            @Override
            public void run() {
                try {
                    Class.forName("com.mysql.jdbc.Driver");//加载数据库驱动
                    connection = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/bifrost?useSSL=false", "root", "518189aA");//获取连接
                    ps  = connection.prepareStatement("select * from "+tname);
                    ResultSet resultSet = ps.executeQuery();
                    while (resultSet.next()) {
                        String userId = resultSet.getString("userId");
                        String city = resultSet.getString("city");
                        map.put(userId,city);
                    }
                    if (connection != null) {
                        connection.close();
                    }
                    if (ps != null) {
                        ps.close();
                    }
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        };
        new Timer("LoadTimer").scheduleAtFixedRate(timerTask,0,2000l);
    }

    @Override
    public void flatMap(String value, Collector out) throws Exception {
        String[] split = value.split(",");
        String key = split[0];
        String mapValue = map.get(key);
        String result = value + ","+key+","+mapValue;
        out.collect(result);
    }
}