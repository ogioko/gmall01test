package com.yalong.gmall01.realtime.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.yalong.gmall01.realtime.bean.DauInfo
import com.yalong.gmall01.realtime.utils.{MyEsUtil, MykafkaUtil, OffsetManagerUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer


/**
 * @auther levithanlee
 * @ccreate${year}-${month}-${day}${time}
 */
object DauApp {

  /*
  * 手动后置提交偏移量
  *
  * 1：  读取偏移量的初始值
  *     从Redis中读取偏移量的数据
  *      偏移量在Redis中以
  *       主题---消费者组---分区---
  *
  * 2：加载数据
  *
  * 3：获取偏移量结束点
  *
  * 4：存储偏移量
  *
  * */

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("dau_app")
    //1 业务对时效的需求  2 处理业务的计算时间  尽量保证周期内可以处理完当前批次
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ODS_BASE_LOG"
    val groupid = "dau_app_group"

    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupid)

    var inputDstream: InputDStream[ConsumerRecord[String, String]] = null

    if (offsetMap == null) {
      inputDstream = MykafkaUtil.getKafkaStream(topic, ssc, groupid)
    } else {
      inputDstream = MykafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupid)

    }

    var offsetRanges: Array[OffsetRange] = null //dirver

    //从流中顺手牵羊吗本批次偏移量结束点存入全局变量

    val inputDstreamWithOffsetDstream: DStream[ConsumerRecord[String, String]] = inputDstream.transform { rdd =>
      //dirver 中周期性执行，或者从rdd中提取数据，例如偏移量
      val hasOffsetRanges: HasOffsetRanges = rdd.asInstanceOf[HasOffsetRanges]
      offsetRanges = hasOffsetRanges.offsetRanges

      rdd
    }

    //把ts 转换成日期 和小时 为后续便于处理
    val jsonObjDstream: DStream[JSONObject] = inputDstreamWithOffsetDstream.map { record =>
      val jsonString: String = record.value()
      val jSONObject: JSONObject = JSON.parseObject(jsonString)
      //把时间戳转换成 日期和小时字段
      val ts: lang.Long = jSONObject.getLong("ts")
      val dateHourStr: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(ts))
      val dateHour: Array[String] = dateHourStr.split(" ")
      val date: String = dateHour(0)
      val hour: String = dateHour(1)
      jSONObject.put("dt", date)
      jSONObject.put("hr", hour)
      jSONObject

    }
    //  map(record=>record.value)
    //筛选用户的最基本活跃行为
    val firstPageJsonDsteam: DStream[JSONObject] = jsonObjDstream.filter {
      jsonObj =>
        val pageJsonObj: JSONObject = jsonObj.getJSONObject("page")
        if (pageJsonObj != null) {
          val lastPageId: String = pageJsonObj.getString("last_page_id")
          if (lastPageId == null || lastPageId == 0) {
            true
          } else {
            false
          }
        } else {
          false
        }
    }
    firstPageJsonDsteam.cache()
    firstPageJsonDsteam.count().print()


    val dauDstream: DStream[JSONObject] = firstPageJsonDsteam.mapPartitions { jsonObjItr =>
      val jedis: Jedis = RedisUtil.getJedisClient //该 批次 分区 执行一次
      val filterList: ListBuffer[JSONObject] = ListBuffer[JSONObject]()

      for (jsonObj <- jsonObjItr) {
        //提取对象中的mid
        val mid: String = jsonObj.getJSONObject("common").getString("mid")
        val dt: String = jsonObj.getString("dt")
        //查询列表中是否有该mid
        //设计定义 已访问列表

        val key = "dau:" + dt
        val isNew: lang.Long = jedis.sadd(key, mid)
        jedis.expire(key, 3600 * 24)

        //如果有旧的，放弃，新的，保留 、、插入到列表中
        if (isNew == 1L) {
          filterList.append(jsonObj)
        }
      }
      jedis.close()
      filterList.toIterator
    }

    //dauDstream.count().print()
    dauDstream.foreachRDD { rdd =>

      rdd.foreachPartition { jsonObjItr =>
        //jsonObjItr作为整体保存，批量保存
        //精确一次性消费实现，幂等性+后置偏移量提交
        /*        for(jsonObj <- jsonObjItr){
                  println(jsonObj)
                }*/
        val docList: List[JSONObject] = jsonObjItr.toList
        //判断是否为空
        if (docList.size > 0) {
          //取当天日期
        val dateFormat = new SimpleDateFormat("yyyyMMdd")
        val dt: String = dateFormat.format(new Date())

          //选择id的条件，1 必须保证在索引（table）中唯一，2，不同时间，不同情况下提交不会变化
          val docWithIdList: List[(String, DauInfo)] = docList.map { jsonObj =>
            val commonJsonObj: JSONObject = jsonObj.getJSONObject("common")
            val mid: String = jsonObj.getJSONObject("common").getString("mid")
             val uid: String = jsonObj.getJSONObject("common").getString("uid")
             val ar: String = jsonObj.getJSONObject("common").getString("ar")
             val ch: String = jsonObj.getJSONObject("common").getString("ch")
             val vc: String = jsonObj.getJSONObject("common").getString("vc")
             val dt: String = jsonObj.getString("dt")
             val hr: String = jsonObj.getString("hr")
             val ts: Long = jsonObj.getLong("ts")
            val dauInfo = DauInfo(mid,uid,ar,ch,vc,dt,hr,ts)
            (mid, dauInfo)
          }
          
          //幂等性保存？给每条数据提供一个id
          MyEsUtil.saveBulk("gmall0921_dau_info_" + dt, docWithIdList)
        }
        // OffsetManagerUtil.saveOffset(topic,groupid,offsetRanges )  ex 每批次，分区
      }
      OffsetManagerUtil.saveOffset(topic, groupid, offsetRanges) //dr 周期性执行，driver中存driver的全局数据
      println("AAAA")
    }
    // OffsetManagerUtil.saveOffset(topic,groupid,offsetRanges ) rd  只执行一次
    println("bbbbb")
    ssc.start()
    ssc.awaitTermination()


  }
}
