package com.atguigu.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
@author 余永蓬
@create 2020-08-01 14:29
*/ object Test02_Rdd_Memory {
  def main(args: Array[String]): Unit = {

    //TODO 从内存中创建RDD
    val conf = new SparkConf().setMaster("local").setAppName("RddCreate")
    val sc = new SparkContext(conf)

    val list = List(1, 2, 3, 4)
    //parallelize: 可以将集合数据作为数据处理的数据源使用
    //parallelize方法可以创建RDD，并指明RDD中数据的类型
    //parallelize表示并行，但是从代码上不容易理解并行的概念

    //makeRDD:生成RDD 理解更容易。推荐
    //makeRDD是间接调用parallelize，所有没区别
    val listRDD: RDD[Int] = sc.makeRDD(list)


    listRDD.collect().foreach(println)

    sc.stop()


  }

}
