package com.atguigu.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
@author 余永蓬
@create 2020-08-04 10:35
*/ object Filter {
  def main(args: Array[String]): Unit = {

    //TODO 过滤 分区不变， 分区数据可能不均衡，生产中可能会数据倾斜
    val conf = new SparkConf().setMaster("local[*]").setAppName("RddCreate")
    val sc = new SparkContext(conf)

    //TODO 小功能：从服务器日志数据apache.log中获取2015年5月17日的请求路径
    val listRDD: RDD[String] = sc.textFile("D:\\IDEAProjectLocation\\Spark\\Spark-Core\\src\\main\\resources\\apache.log")
    val timeRDD=listRDD.map(
      log=>{
        val data=log.split(" ")
        (data(3),data(6))
      }
    )
     val filterRDD=timeRDD.filter{
       case (time,url)=>{
         time.startsWith("17/05/2015")
       }
     }
    filterRDD.collect()

    sc.stop()
  }

}
