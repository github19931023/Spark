package com.atguigu.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/*
@author 余永蓬
@create 2020-07-31 10:21
*/ object Test07_WordCount {
  def main(args: Array[String]): Unit = {

    //1.建立和Spark的联系
    //创建spark环境对象(上下文对象)，现需要指定环境配置
    val conf=new SparkConf().setMaster("local").setAppName("wordcount")
    val sc=new SparkContext(conf)
    //2.实现操作
    val line: RDD[String] = sc.textFile("D:\\IDEAProjectLocation\\Spark\\Spark-Core\\src\\main\\resources\\wordCount")
    //val res=line.flatMap(_.split(" ")).groupBy(word=> word).map(t=> (t._1,t._2.size)).collect().foreach(println)
    val res=line.flatMap(_.split(" ")).map((_,1)).countByKey().foreach(println)


  }


}
