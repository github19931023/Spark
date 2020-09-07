package com.atguigu.spark.streaming

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer
import scala.util.Random


/*
@author 余永蓬
@create 2020-08-14 15:05
*/ object SparkStreaming_MockData {

  def main(args: Array[String]): Unit = {

    //1.初始化Spark配置信息
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamWordCount")

    //2.初始化SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))
//
//     TODO 模拟的数据
//     *
//     * 格式 ：timestamp area city userid adid
//     * 某个时间点 某个地区 某个城市 某个用户 某个广告
//
// TODO 生成实时模拟数据
// 将生成的模拟数据发送到Kafka中

    //TODO  生成用户广告的点击数据
    val props=new Properties()

    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
   val producer= new KafkaProducer[String,String](props)

    while(true){
      for(data<- genData()) {
        //Todo 循环开始把生成的数据不断的发送kafka topic中
        producer.send( new ProducerRecord[String, String]("atguigu0421", data) )
        println(data)
      }
      Thread.sleep(2000)
    }

  }

//todo 生产数据的方法

  private def genData()={
    val areaList = List( "华北", "东北", "华南" )
    val cityList = List( "北京", "上海", "深圳" )

    val list=ListBuffer[String]()

    // 格式 ：timestamp area city userid adid
    // 某个时间点 某个地区 某个城市 某个用户 某个广告
    for(i<- 1 to new Random().nextInt(50)){

      val area=areaList(new Random().nextInt(3))
      val city = cityList( new Random().nextInt(3) )
      val userid = 1 + new Random().nextInt(6)
      val adid = 1 + new Random().nextInt(6)

      //todo 往集合list里面追加模拟的数据
      list.append(s"${System.currentTimeMillis()} ${area} ${city} ${userid} ${adid}")
    }

    //返回list
    list

  }


}
