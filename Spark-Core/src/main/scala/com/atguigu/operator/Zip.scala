package com.atguigu.operator

import org.apache.spark.{SparkConf, SparkContext}

/*
@author 余永蓬
@create 2020-08-04 11:28
*/ object Zip {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("RddCreate")
    val sc = new SparkContext(conf)

    //TODO 两个value型的值，就是集合
    //TODO 两个集合类型必须一致，不然报错
    // zip要求： 分区数量得相同，每个分区的元素个数相同
   val rdd= sc.makeRDD(List(1,2,3,4),2)
    val rdd1= sc.makeRDD(List("2","3","4","5"),2)

    val res=rdd.zip(rdd1) //SparkException: Can only zip RDDs with same number of elements in each partition
      res.collect().foreach(println)



    sc.stop()
  }

}
