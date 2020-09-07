package com.atguigu.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
@author 余永蓬
@create 2020-08-01 14:29
*/ object Test01_Operator_Transform {
  def main(args: Array[String]): Unit = {

    //TODO 从内存中创建RDD
    val conf = new SparkConf().setMaster("local[*]").setAppName("RddCreate")
    val sc = new SparkContext(conf)
    //获取日志文件每一个用户请求信息，获取用户请求路径
    val logRDD: RDD[String] = sc.textFile("D:\\IDEAProjectLocation\\Spark\\Spark-Core\\src\\main\\resources\\apache.log")

    val urlRDD: RDD[String] = logRDD.map(
      log => {
        val datas: Array[String] = log.split(" ")
        datas(6)

      }
    )

    urlRDD.collect().foreach(println)






    sc.stop()
   


  }

}
