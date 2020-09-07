package com.atguigu.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
@author 余永蓬
@create 2020-08-01 14:29
*/ object Test01_Operator_Transform3 {
  def main(args: Array[String]): Unit = {

    //TODO 从内存中创建RDD
    val conf = new SparkConf().setMaster("local[*]").setAppName("RddCreate")
    val sc = new SparkContext(conf)

    val list: List[Int] = List(1,2,3,4)
    val rdd: RDD[Int] = sc.makeRDD(list,2)
    //找出每个分区的最大值
    //mapPartitions 算子要求匿名函数中参数为可迭代的集合
    //匿名函数的返回值类型应该为可迭代的集合


    //map数据不会变，mapPartitions数据会增多或减少
    //map算子处理一条数据就没用了，数据会被内存回收，mapPartitions直到分区所有数据处理完才会
    //一起回收。缺点就是可能导致内存不够用溢出。


    //分区间无序，  分区内有序
    val newRDD: RDD[Int] = rdd.mapPartitions(
      datas => {
        List(datas.max).iterator
      }
    )
    newRDD.saveAsTextFile("out1")




    sc.stop()
   


  }

}
