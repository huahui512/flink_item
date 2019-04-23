package com.missfresh.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

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
}
