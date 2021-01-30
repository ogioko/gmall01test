package com.yalong.gmall01.realtime.utils

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

import scala.collection.mutable

/**
 * @auther levithanlee
 * @ccreate${year}-${month}-${day}${time}
 */
object OffsetManagerUtil {


  //读取偏移量
  def getOffset(topic: String, groupID: String): Map[TopicPartition, Long] = {
    val jedis: Jedis = RedisUtil.getJedisClient

    //topic:consumer_group ,  partition, offset , hgetall
    val offsetKey = topic + ":" + groupID
    val offsetMapOri: util.Map[String, String] = jedis.hgetAll(offsetKey)


    jedis.close()

    if (offsetMapOri != null&&offsetMapOri.size()>0){

      import collection.JavaConverters._

      //把Redis中取出的结构，转换为kafka要求的结构
      val offsetMapForKafka: Map[TopicPartition, Long] = offsetMapOri.asScala.map {
        case (partitionStr, offsetStr) =>
          val topicPartition: TopicPartition = new TopicPartition(topic, partitionStr.toInt)

          (topicPartition, offsetStr.toLong)

      }.toMap
      println("读取起始偏移量: "+ offsetMapForKafka )
      offsetMapForKafka
    }else{
        null
    }

}
  //偏移量写入redis
  def saveOffset(topic:String ,groupID:String,offsetRanges:Array[OffsetRange]):Unit ={
    val jedis: Jedis = RedisUtil.getJedisClient
    //偏移量存到Redis type  ：  hash， 写入 api:
    val offsetKey = topic+":"+groupID


val offsetMapForRedis = new util.HashMap[String,String]()

    for ( offsetRange <- offsetRanges ) {
      val partition: Int = offsetRange.partition  //分区 号 index
      val offset: Long = offsetRange.untilOffset  // 偏移量结束点

      offsetMapForRedis.put(partition.toString,offset.toString)
    }

    println("写入结束点："+ offsetMapForRedis)
    jedis.hmset(offsetKey,offsetMapForRedis)
    jedis.close()



  }

}
