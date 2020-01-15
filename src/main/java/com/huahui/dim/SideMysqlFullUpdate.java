package com.huahui.dim;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by admin on 2019-10-31 11:25
 */
public class SideMysqlFullUpdate extends RichFlatMapFunction<String,String> {
    //存储维度数据的集合
    Map<String, String> sideMap=null;
    AtomicReference atomicSideMap=null;
    ScheduledExecutorService executor=null;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        sideMap = new HashMap<>();
        //使用原子性保证维度数据读写一致
        atomicSideMap   = new AtomicReference(sideMap);
        executor= Executors.newSingleThreadScheduledExecutor();
        //定期全量更新维度数据
        executor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    load();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        },5,5, TimeUnit.MINUTES);
    }

    @Override
    public void flatMap(String value, Collector<String> out) throws Exception {
        JSONObject jsonObject = JSONObject.parseObject(value);
        String id = jsonObject.getString("id");
        sideMap = (Map<String,String>) atomicSideMap.get();
        //获取维度数据
        String aid = sideMap.get(id);
        out.collect(aid);
    }
    public void load() throws ClassNotFoundException, SQLException {


        Class.forName("com.mysql.jdbc.Driver");
        Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/paul", "root", "123456");
        String  sql = "select aid,tid from ads";
        PreparedStatement statement = con.prepareStatement(sql);
        ResultSet rs = statement.executeQuery();
        //全量更新维度数据到内存
        while (rs.next()) {
            String aid = rs.getString("aid");
            String tid = rs.getString("tid");
            sideMap.put(tid, aid);
        }
        atomicSideMap.set(sideMap);
        con.close();
    }}

