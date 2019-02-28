package com.missfresh.util;
import java.sql.*;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @author wangzhihua
 * @date 2019-02-28 11:33
 */
public class GetInfo {
    public  static String getTypeInfo() throws SQLException, ClassNotFoundException {
        String URL="jdbc:mysql://127.0.0.1:3306/bifrost";
        String USER="root";
        String PASSWORD="518189aA";
        //1.加载驱动程序
        Class.forName("com.mysql.jdbc.Driver");
        //2.获得数据库链接
        Connection conn=DriverManager.getConnection(URL, USER, PASSWORD);
        //3.通过数据库的连接操作数据库，实现增删改查（使用Statement类）
        Statement st=conn.createStatement();
        //获取所有字段
        ResultSet rs=st.executeQuery("select * from ra_user");
        //获取表的字段名和字段类型
        ResultSetMetaData metaData = rs.getMetaData();
        StringBuffer sb=new StringBuffer();
        //从1开始跳过id
        for (int i = 1; i < metaData.getColumnCount(); i++) {
            // resultSet数据下标从1开始
            String columnName = metaData.getColumnName(i + 1);
            int columnType = metaData.getColumnType(i + 1);
            String typeInfo=null;
            if (Types.INTEGER == columnType) {
                typeInfo="int";
            } else if (Types.VARCHAR == columnType) {
                typeInfo="String";
            }else if (Types.DOUBLE == columnType) {
                typeInfo="double";
            }else if (Types.FLOAT == columnType) {
                typeInfo="float";
            }else if (Types.BIGINT == columnType) {
                typeInfo="int";
            }else if (Types.TINYINT == columnType) {
                typeInfo="int";
            }else  {
                typeInfo="String";
            }
            //拼接成需要的字符串
            String str=columnName+"&"+typeInfo;
            sb.append(str);
            sb.append(",");
        }
        //去掉最后一个逗号
        String substring = sb.substring(0, sb.length() - 1);
        String TypeInfo = substring.toString();
        //关闭连接
        rs.close();
        st.close();
        conn.close();
        return TypeInfo;
    }

    public static void main(String[] args) {
        try {
            System.out.println(getTypeInfo());
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }



}
