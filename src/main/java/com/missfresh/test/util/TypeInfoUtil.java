package com.missfresh.test.util;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;

import java.sql.*;

public class TypeInfoUtil {
    /**
     * @return userId&int,categoryId&String,itemId&String,behavior&String,timestamp&String
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public static String getFieldAndTypeStr() throws SQLException, ClassNotFoundException {
        String url = "jdbc:mysql://10.2.40.10:3306/bifrost";
        String user = "root";
        String password = "cfiKRecV1tKW!";
        //1.加载驱动程序
        Class.forName("com.mysql.jdbc.Driver");
        //2.获得数据库链接
        Connection conn = DriverManager.getConnection(url, user, password);
        //3.通过数据库的连接操作数据库，实现增删改查（使用Statement类）
        Statement st = conn.createStatement();


        //获取所有字段
        ResultSet rs = st.executeQuery("select * from ra_user");
        //获取表的字段名和字段类型
        ResultSetMetaData metaData = rs.getMetaData();
        StringBuilder sb = new StringBuilder();
        //从1开始跳过id
        for (int i = 1; i < metaData.getColumnCount(); i++) {
            // resultSet数据下标从1开始
            String columnName = metaData.getColumnName(i + 1);
            int columnType = metaData.getColumnType(i + 1);
            String typeInfo;
            if (Types.INTEGER == columnType) {
                typeInfo = "int";
            } else if (Types.VARCHAR == columnType) {
                typeInfo = "String";
            } else if (Types.DOUBLE == columnType) {
                typeInfo = "double";
            } else if (Types.FLOAT == columnType) {
                typeInfo = "float";
            } else if (Types.BIGINT == columnType) {
                typeInfo = "int";
            } else if (Types.TINYINT == columnType) {
                typeInfo = "int";
            } else {
                typeInfo = "String";
            }
            //拼接成需要的字符串
            String str = columnName + "&" + typeInfo;
            sb.append(str);
            sb.append(",");
        }
        //去掉最后一个逗号
        String substring = sb.substring(0, sb.length() - 1);
        //关闭连接
        rs.close();
        st.close();
        conn.close();
        return substring;
    }

    /**
     * @param infoStr behavior&String, userId&String, categoryId&String, itemId&String, timestamp&String
     * @return RowTypeInfo(types, fieldNames)
     */
    public static RowTypeInfo getRowTypeInfo(String infoStr) {
        //解析字符串，通过循环把字段名和字段类型存放到对应的数组中
        String[] typeInfoArr = infoStr.split(",");
        String[] fieldNames = new String[typeInfoArr.length];
        TypeInformation[] typeInformations = new TypeInformation[typeInfoArr.length];
        for (int i = 0; i < typeInfoArr.length; i++) {
            String fieldName = typeInfoArr[i].split("&")[0];
            fieldNames[i] = fieldName;
            String typeStr = typeInfoArr[i].split("&")[1];

            switch (typeStr) {
                case "int":
                    typeInformations[i] = org.apache.flink.table.api.Types.INT();
                    break;
                case "long":
                    typeInformations[i] = org.apache.flink.table.api.Types.LONG();
                    break;
                case "double":
                    typeInformations[i] = org.apache.flink.table.api.Types.DOUBLE();
                    break;
                case "float":
                    typeInformations[i] = org.apache.flink.table.api.Types.FLOAT();
                    break;
                default:
                    typeInformations[i] = org.apache.flink.table.api.Types.STRING();
            }
        }
        //返回Row的字段名和字段类型
        return new RowTypeInfo(typeInformations, fieldNames);
    }


    public static void main(String[] args) {
        try {
            System.out.println(getFieldAndTypeStr());
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }


}
