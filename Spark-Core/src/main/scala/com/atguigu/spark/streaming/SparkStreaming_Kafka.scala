package com.atguigu.spark.streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable


/*
@author 余永蓬
@create 2020-08-14 15:05
*/ object SparkStreaming_Kafka {
  def main(args: Array[String]): Unit = {

    //1.初始化Spark配置信息
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamWordCount")

    //2.初始化SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))
  //TODO 需求：循环创建几个RDD，将RDD放入队列。通过SparkStream创建Dstream，计算WordCount
    // TODO 数据处理


    //3.定义Kafka参数
   val kafkaParams= Map[String,Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG-> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "atguigu",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    //4.读取Kafka数据创建DStream
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent, //位置策略，让excutor和kafka在同一个Node上
      ConsumerStrategies.Subscribe[String, String](Set("atguigu0421"), kafkaParams)
    )

    

    //5.将每条消息的KV取出

    val valueDStream=kafkaDStream.map(record => record.value())
    //6.计算WordCount
    valueDStream.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .print()




    ssc.start()
    ssc.awaitTermination()



  }

}
