package com.missfresh.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author wangzhihua
 * @date 2019-01-18 11:19
 */
public class HbaseUtil {
     public static Configuration conf = null;
     public  static Connection connection = null;
     public static Admin admin = null;

    /**
     * @desc 取得连接
     */
    public static void setConf(String quorum, String port) {
        try {
            conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", quorum);//zookeeper地址
            conf.set("hbase.zookeeper.property.clientPort", port);
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * @desc 连关闭接
     */
    public static void close() {
        try {
            if (connection != null) {
                connection.close();
            }
            if (admin != null) {
                admin.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * @desc 创建表
     */
    public static void createTable(String tableName, String columnFamily) {


        try {
            TableName tbName = TableName.valueOf(tableName);
            if (!admin.tableExists(tbName)) {
                HTableDescriptor hTableDescriptor = new HTableDescriptor(tbName);
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(columnFamily);
                hTableDescriptor.addFamily(hColumnDescriptor);
                admin.createTable(hTableDescriptor);
            }
        } catch (IOException e) {
            e.printStackTrace();

        }

        /**
         * 添加多条记录
         */
      /*  public static void addMoreRecord (String tableName, String family, String
        qualifier, List < String > rowList, String value){
            Table table = null;
            try {
                table = connection.getTable(TableName.valueOf(tableName));

                List<Put> puts = new ArrayList<>();
                Put put = null;
                for (int i = 0; i < rowList.size(); i++) {
                    put = new Put(Bytes.toBytes(rowList.get(i)));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));

                    puts.add(put);
                }
                table.put(puts);

            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (table != null) {
                    try {
                        table.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }

        }*/


    }
}