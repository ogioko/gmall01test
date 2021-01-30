package com.yalong.gmall01.realtime.app

import java.{lang, util}
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.yalong.gmall01.realtime.bean.DauInfo
import com.yalong.gmall01.realtime.utils.{HbaseUtil, MyEsUtil, MykafkaSink, MykafkaUtil, OffsetManagerUtil, RedisUtil}
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
 * @auther levithanlee
 * @ccreate${year}-${month}-${day}${time}
 */
object BaseDBCanalApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("base_db_canal_app")
    //1 业务对时效的需求  2 处理业务的计算时间  尽量保证周期内可以处理完当前批次
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ODS_BASE_DB_C"
    val groupid = "base_db_canal_group"

    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupid)

    var inputDstream: InputDStream[ConsumerRecord[String, String]] = null

    if (offsetMap == null) {
      inputDstream = MykafkaUtil.getKafkaStream(topic, ssc, groupid)
    } else {
      inputDstream = MykafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupid)

    }
//获取偏移量的结束点
    var offsetRanges: Array[OffsetRange] = null //dirver

    //从流中顺手牵羊吗本批次偏移量结束点存入全局变量

    val inputDstreamWithOffsetDstream: DStream[ConsumerRecord[String, String]] = inputDstream.transform { rdd =>
      //dirver 中周期性执行，或者从rdd中提取数据，例如偏移量
      val hasOffsetRanges: HasOffsetRanges = rdd.asInstanceOf[HasOffsetRanges]
      offsetRanges = hasOffsetRanges.offsetRanges

      rdd
    }
    //转换格式为jsonObj 便于中间业务处理
      val jsonObjDstream: DStream[JSONObject] = inputDstreamWithOffsetDstream.map(record=> JSON.parseObject(record.value()))


    //table,type,pkName,data-->kafka / hbase
    //清单：常量，配置文件，数据库参数
    //      事实表，：写入kafka -- 不同的表进 不同topic ? table  value? date
    //      维度表 ：写入HBASE --hbase  table? row key? column ? family? namespace?
    jsonObjDstream.foreachRDD{ rdd=>

        rdd.foreachPartition{ jsonItr=>

          //清单
          val dimTables=Array("user_info","base_province")
          val factTable = Array("order_info","order_detail")

            for ( jsonObj <- jsonItr) {
              val table: String = jsonObj.getString("table")
              val optType: String = jsonObj.getString("type")
              val pkNames: JSONArray = jsonObj.getJSONArray("pkNames")
              val dataArr: JSONArray = jsonObj.getJSONArray("data")

              //按维度处理 -- (hbase)
              //写入HBASE --hbase  table? row key? column ? family? namespace?
              //rowkey是否打散？视情况，大表打散，小表不用，需要打散，预分区，不需要打散不预分区
              //用户表使用rowkey查询，做预分区，省地表可以整表查，无需预分区
              //取得数据的主键，先补零再反转
              if (dimTables.contains(table)){
                val pkName: String =  pkNames.getString(0)

                import  collection.JavaConverters._
                for (data <- dataArr.asScala ) {
                  val dataJsonObj: JSONObject = data.asInstanceOf[JSONObject]
                  val pk: String = dataJsonObj.getString(pkName)
                  // 用数据的主键 1 先补0 补的位数预计是数据增上限    //再反转
                  val rowkey: String = StringUtils.leftPad(pk,10,"0").reverse
                  //val namespace:String="GMALL0921"
                  val hbaseTable:String="DIM_"+table.toUpperCase

                  val dateMap: util.Map[String, AnyRef] = dataJsonObj.getInnerMap

                  HbaseUtil.put(hbaseTable,rowkey,dateMap)
                  //HbaseUtil.put 吧数据存入到Hbase中

                }


              }
              if (factTable.contains((table))){
                //按事实去处理--（kafka）
                //事实表，：写入kafka -- 不同的表进 不同topic ? table+optType  value? date
                var opt:String =null
                if (optType.equals("INSERT")){
                  opt = "I"
                }else if (optType.equals("UPDATE")){
                  opt= "U"
                }else if (optType.equals("DELETE")){
                  opt="D"
                }

              //kafka发送工具？
                val topic = "DWD"+table.toUpperCase()+"_"+opt
                import collection.JavaConverters._
                for ( data<- dataArr.asScala ) {
                  val dataJsonObj: JSONObject  = data.asInstanceOf[JSONObject]
                  MykafkaSink.send(topic,dataJsonObj.toJSONString)
                }

              }

              println(jsonObj)
            }
        }
        OffsetManagerUtil.saveOffset(topic, groupid, offsetRanges)


    }

    ssc.start()
    ssc.awaitTermination()
  }
}
