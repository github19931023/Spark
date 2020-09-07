package com.atguigu.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
@author 余永蓬
@create 2020-08-01 14:29
*/ object Test03_RDD_File {
  def main(args: Array[String]): Unit = {

    //TODO 从文件中创建RDD
    val conf = new SparkConf().setMaster("local").setAppName("RddCreate")
    val sc = new SparkContext(conf)


    //parallelize: 可以将集合数据作为数据处理的数据源使用
    //parallelize方法可以创建RDD，并指明RDD中数据的类型
    //parallelize表示并行，但是从代码上不容易理解并行的概念

    //makeRDD:生成RDD 理解更容易。推荐
    //makeRDD是间接调用parallelize，所有没区别

    //TODO 将文件作为数据源
    //textFile 一行一行读
    //本地路径local，  相对路径，  绝对路径
    //spark读取文件时，可以指明文件的规则（通配符）
    val fileRDD: RDD[String] = sc.textFile("D:\\IDEAProjectLocation\\Spark\\Spark-Core\\src\\main\\resources\\wordCount")



    fileRDD.collect().foreach(println)

    sc.stop()


  }

}
