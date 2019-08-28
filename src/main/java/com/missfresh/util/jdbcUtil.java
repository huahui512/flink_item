package com.missfresh.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Map;
import java.util.Set;

/**
 * @author wangzhihua
 * @date 2019-04-12 15:05
 */
public class jdbcUtil  implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(jdbcUtil.class);
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static String osName = System.getProperties().getProperty("os.name");
    private static String DB_URL;
    private static String USER;
    private static String PASS;

    static {
        if ("Linux".equals(osName)) {
            DB_URL = "jdbc:mysql://10.3.34.17:3306/bifrost";
            USER = "bifrost_admin";
            PASS = "bifrost3AHEd5OV";
        } else {
            DB_URL = "jdbc:mysql://127.0.0.1:3306/bifrost";
            USER = "root";
            PASS = "cfiKRecV1tKW!";
        }
    }

    public static Connection getConn() {
        Connection conn = null;
        try {
            Class.forName(JDBC_DRIVER);
            System.out.println("连接数据库...");
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }

    public static void getData() {


    }

    @Override
    public void run() {
        Connection conn = jdbcUtil.getConn();
        try {PreparedStatement ps = conn.prepareStatement("select job_name from rt_job") ;

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        String abinfo="[{\"exp_952e942b-8872-44fa-9625-b5ffb1fe71d0\":\"group_89459ab5-fa37-455b-aadd-07fd55bbe118\"},{\"e4g23xeh\":\"gz4npeph\"},{\"ehcvffl0\":\"gxqwrr3r\"},{\"ejbu2v61\":\"gyqmfsir\"},{\"eg0uz58h\":\"g5bkglr0\"},{\"exo9rmb7\":\"gzfy0gc5\"},{\"ejrtyymj\":\"g8w77slv\"},{\"emp0uqee\":\"g5f2cqit\"},{\"evgbs6uc\":\"g6m464yt\"},{\"exp_d4025b53-f095-4538-b00c-0d3e8ef09835\":\"group_de473259-8464-4672-b6c1-f68b155fbe9d\"},{\"e8lpaiir\":\"ggrksif2\"}]";
        JSONArray jsonArray = JSON.parseArray(abinfo);
        for (int i = 0; i < jsonArray.size(); i++) {
            JSONObject jsonStr = (JSONObject) jsonArray.get(i);
            Set<Map.Entry<String, Object>> entries = jsonStr.entrySet();
            for (Map.Entry<String, Object> entry : entries) {
                System.out.println(entry.getKey() + "," + entry.getValue());
            }
        }
    }
}
