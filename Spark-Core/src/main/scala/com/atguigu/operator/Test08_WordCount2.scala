package com.atguigu.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
@author 余永蓬
@create 2020-08-04 9:29

TODO 需求:求log文件中某一时间段内的访问量

*/ object Test08_WordCount2 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("RddCreate")
    val sc = new SparkContext(conf)

    val listRDD: RDD[String] = sc.textFile("D:\\IDEAProjectLocation\\Spark\\Spark-Core\\src\\main\\resources\\apache.log")
    val timeRDD=listRDD.map(
      log=>{
        val data=log.split(" ")
        data(3)
      }
    )

    timeRDD.groupBy(
      time=>{
        val hour=time.substring(11,13)

       println("hour=" +hour )
        hour   //返回

      }
    ).collect()


    sc.stop()
  }

}
