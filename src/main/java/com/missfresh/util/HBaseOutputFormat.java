package com.missfresh.util;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @author wangzhihua
 * @date 2019-01-18 15:35
 */
/*public class HBaseOutputFormat implements OutputFormat<Tuple5<Long, Long, Long, String, Long>> {
    private org.apache.hadoop.conf.Configuration conf = null;
    private Connection conn = null;
    private Table table = null;

    @Override
    public void configure(Configuration parameters) {
    }


  *//*  @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        HbaseUtil.setConf("10.2.40.12,10.2.40.11,10.2.40.8", "2181");
        conn = HbaseUtil.connection;
        HbaseUtil.createTable("flink_test2","info");
    }*//*

    @Override
    public void writeRecord(Tuple5<Long, Long, Long, String, Long> record) throws IOException {
        Put put = new Put(Bytes.toBytes(record.f0+record.f4));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("uerid"), Bytes.toBytes(record.f0));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("behavior"), Bytes.toBytes(record.f3));
        ArrayList<Put> putList = new ArrayList<>();
        putList.add(put);
        //设置缓存1m，当达到1m时数据会自动刷到hbase
        BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf("flink_test2"));
        params.writeBufferSize(1024 * 1024); //设置缓存的大小
        BufferedMutator mutator = conn.getBufferedMutator(params);
        mutator.mutate(putList);
        mutator.flush();
        putList.clear();
    }

    @Override
    public void close() throws IOException {
        if (table != null) {
            table.close();
        }
        if (conn != null) {
            conn.close();
        }
    }



}*/
