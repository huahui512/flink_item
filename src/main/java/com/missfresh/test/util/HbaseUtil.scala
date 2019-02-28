package com.missfresh.test.util

import com.esotericsoftware.kryo.Kryo
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable

/**
  * Created by wangtaiyang on 2018/8/10. 
  */
object HbaseUtil {
  var conf: Configuration = _
  //线程池
  lazy val connection: Connection = ConnectionFactory.createConnection(conf)
  lazy val admin: Admin = connection.getAdmin

  /**
    * hbase conf
    *
    * @param quorum hbase的zk地址
    * @param port   zk端口2181
    * @return
    */
  def setConf(quorum: String, port: String): Unit = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", quorum)
    conf.set("hbase.zookeeper.property.clientPort", port)
    this.conf = conf
  }

  /**
    * 如果不存在就创建表
    *
    * @param tableName    命名空间：表名
    * @param columnFamily 列族
    */
  def createTable(tableName: String, columnFamily: String): Unit = {
    val tbName = TableName.valueOf(tableName)
    if (!admin.tableExists(tbName)) {
      val htableDescriptor = new HTableDescriptor(tbName)
      val hcolumnDescriptor = new HColumnDescriptor(columnFamily)
      htableDescriptor.addFamily(hcolumnDescriptor)
      admin.createTable(htableDescriptor)
    }
  }

  def hbaseScan(tableName: String): ResultScanner = {
    val scan = new Scan()
    val table = connection.getTable(TableName.valueOf(tableName))
    table.getScanner(scan)
    //    val scanner: CellScanner = rs.next().cellScanner()
  }

  /**
    * 获取hbase单元格内容
    *
    * @param tableName 命名空间：表名
    * @param rowKey    rowkey
    * @return 返回单元格组成的List
    */
  def getCell(tableName: String, rowKey: String): mutable.Buffer[Cell] = {
    val get = new Get(Bytes.toBytes(rowKey))
    val table = connection.getTable(TableName.valueOf(tableName))
    val result: Result = table.get(get)
    import scala.collection.JavaConverters._
    result.listCells().asScala
  }

  /**
    * 单条插入
    *
    * @param tableName 命名空间：表名
    * @param rowKey    rowkey
    * @param family    列族
    * @param qualifier column列
    * @param value     列值
    */
  def singlePut(tableName: String, rowKey: String, family: String, qualifier: String, value: String): Unit = {
    //向表中插入数据//向表中插入数据
    //a.单个插入
    val put: Put = new Put(Bytes.toBytes(rowKey)) //参数是行健row01
    put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value))

    //获得表对象
    val table: Table = connection.getTable(TableName.valueOf(tableName))

    table.put(put)
    table.close()

  }

  /**
    * 删除数据
    *
    * @param tbName 表名
    * @param row    rowkey
    */
  def deleteByRow(tbName: String, row: String): Unit = {
    val delete = new Delete(Bytes.toBytes(row))
    //    delete.addColumn(Bytes.toBytes("fm2"), Bytes.toBytes("col2"))
    val table = connection.getTable(TableName.valueOf(tbName))
    table.delete(delete)
  }

  def close(): Unit = {
    admin.close()
    connection.close()
  }

  def main(args: Array[String]): Unit = {
    setConf("localhost", "2181")
    try{
      createTable("kafka_offset:topic_offset_range", "info")
      singlePut("kafka_offset:topic_offset_range", "gid_topic_name", "info", "partition0", "200")
      singlePut("kafka_offset:topic_offset_range", "gid_topic_name", "info", "partition1", "300")
      singlePut("kafka_offset:topic_offset_range", "gid_topic_name", "info", "partition2", "100")

    }catch {
      case e:Exception=>println("----")
    }

    this.close()


  }
}
