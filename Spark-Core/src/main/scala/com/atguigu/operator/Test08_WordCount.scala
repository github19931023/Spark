package com.atguigu.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
@author 余永蓬
@create 2020-08-04 9:29
*/ object Test08_WordCount {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("RddCreate")
    val sc = new SparkContext(conf)

    val listRDD: RDD[String] = sc.textFile("")
    val wordRdd = listRDD.flatMap(_.split(" "))
    val groupRDD: RDD[(String, Iterable[String])] = wordRdd.groupBy(word => word)

    groupRDD.map{
      case (word,list)=>{
        (word,list.size)
      }
    }.collect().foreach(println)


    sc.stop()
  }

}
