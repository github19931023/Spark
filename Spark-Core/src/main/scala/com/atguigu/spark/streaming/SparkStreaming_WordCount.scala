package com.atguigu.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}


/*
@author 余永蓬
@create 2020-08-14 15:05
*/ object SparkStreaming_WordCount {
  def main(args: Array[String]): Unit = {

    //1.初始化Spark配置信息
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamWordCount")

    //2.初始化SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //3.从9999端口中采集数据
    //获取数据，封装为离散化数据流对象（封装）
    val DStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)

    //将每一行数据做切分，形成一个个单词
    val wordStreams = DStream.flatMap(_.split(" "))

    //将单词映射成元组（word,1）
    val wordAndOneStreams = wordStreams.map((_, 1))

    //将相同的单词次数做统计
    val wordAndCountStreams = wordAndOneStreams.reduceByKey(_+_)

    //打印
    wordAndCountStreams.print()

    //启动SparkStreamingContext
    ssc.start()
    ssc.awaitTermination()



  }

}
