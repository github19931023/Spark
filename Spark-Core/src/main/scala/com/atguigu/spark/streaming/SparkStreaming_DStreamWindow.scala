package com.atguigu.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}


/*
@author 余永蓬
@create 2020-08-14 15:05
*/ object SparkStreaming_DStreamWindow {
  def main(args: Array[String]): Unit = {

    //1.初始化Spark配置信息
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamWordCount")

    //2.初始化SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))  //数据采集周期

    //3.从9999端口中采集数据
    //获取数据，封装为离散化数据流对象（封装）
    val ds: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)

   val res= ds.map((_,1))
        .window(Seconds(6),Seconds(6))  //窗口时长：计算内容的时间范围；滑动步长：隔多久触发一次计算。

        .reduceByKey(_+_)

    res.print()

    ssc.start()
    ssc.awaitTermination()



  }

}
