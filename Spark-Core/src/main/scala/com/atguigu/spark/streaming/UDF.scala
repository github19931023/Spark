package com.atguigu.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}


/*
@author 余永蓬
@create 2020-08-14 15:05
*/ object UDF {
  def main(args: Array[String]): Unit = {

    //1.初始化Spark配置信息
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamWordCount")

    //2.初始化SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))
//TODO 自定义数据源

   val ds= ssc.receiverStream(new MyReceiver(1*1000))


    ds.print()
    //启动SparkStreamingContext
    ssc.start()
    ssc.awaitTermination()



  }
//自定义数据源 需要继承Receiver，并实现onStart、onStop方法来自定义数据源采集。
  class MyReceiver(sleepTime:Long) extends  Receiver[String](StorageLevel.MEMORY_ONLY ){

  private var  flag=true
  override def onStart(): Unit = {
      //当采集器启动后的操作   //最初启动的时候，调用该方法，作用为：读数据并将数据发送给Spark

      while (flag){
        val s="自定义采集器生成的字符串： "+ System.currentTimeMillis()

        //将字符串数据封装存储，让SparkStreaming可以动态操作

        store(s)

        Thread.sleep(sleepTime)
      }
    }
  //当采集器执行完毕后的操作
    override def onStop(): Unit = {
      flag=false
    }
  }


}
