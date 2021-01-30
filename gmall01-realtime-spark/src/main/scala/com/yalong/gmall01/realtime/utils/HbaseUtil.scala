package com.yalong.gmall01.realtime.utils

import java.io.IOException
import java.util
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer


/**
 * @auther levithanlee
 * @ccreate${year}-${month}-${day}${time}
 */
object HbaseUtil {
  var connection: Connection = null
  private val properties: Properties = PropertiesUtil.load("config.properties")
  val HBASE_SERVER = properties.getProperty("hbase.server")
  val DEFAULT_FAMILY = properties.getProperty("hbase.default.family")
  val NAMESPACE = properties.getProperty("hbase.namespace")


  def put(tableName: String, rowKey: String, columnValueMap: java.util.Map[String, AnyRef]): Unit = {
    if (connection == null) init()
    //连接，表
    val table: Table = connection.getTable(TableName.valueOf(NAMESPACE + ":" + tableName))
    //表--put

    val puts = new util.ArrayList[Put]()
    import collection.JavaConverters._
    for (colValue <- columnValueMap.asScala) {
      val col: String = colValue._1
      val value: AnyRef = colValue._2
      //判断为空字段，不进入
      if (value != null && value.toString.length > 0) {
        val put = new Put(Bytes.toBytes(rowKey))
          .addColumn(Bytes.toBytes(DEFAULT_FAMILY), Bytes.toBytes(col), Bytes.toBytes(value.toString))
        puts.add(put)
      }
    }
    table.put(puts)
  }


  //????
  def init() = {
    if (connection == null) {
      val conf: Configuration = HBaseConfiguration.create
      conf.set("hbase.zookeeper.quorum", HBASE_SERVER)

      val table: Table = null
      try
        connection = ConnectionFactory.createConnection(conf)
      catch {
        case e: IOException =>
          e.printStackTrace()
          throw new RuntimeException("连接zk - hbase失败")
      }
    }


  }
}
