package com.atguigu.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable


/*
@author 余永蓬
@create 2020-08-14 15:05
*/ object SparkStreaming_WordCount_RDD {
  def main(args: Array[String]): Unit = {

    //1.初始化Spark配置信息
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamWordCount")

    //2.初始化SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))
  //TODO 需求：循环创建几个RDD，将RDD放入队列。通过SparkStream创建Dstream，计算WordCount
    // TODO 数据处理

    val rddQueue=new mutable.Queue[RDD[Int]]()
    val inputStream= ssc.queueStream(rddQueue)
    val reduceDS: DStream[Int] = inputStream.reduce(_+_)

    reduceDS.print()

    //启动SparkStreamingContext
    ssc.start()

    while(true){
      //SparkStreaming对SparkCore的核心对象进行了包装
      //可以通过环境对象获取到SparkContext对象
      rddQueue +=ssc.sparkContext.makeRDD(1 to 10)
      Thread.sleep(3000)

    }




    ssc.awaitTermination()



  }

}
