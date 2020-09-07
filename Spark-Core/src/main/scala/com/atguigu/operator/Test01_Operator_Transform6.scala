package com.atguigu.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
@author 余永蓬
@create 2020-08-01 14:29
*/ object Test01_Operator_Transform6 {
  def main(args: Array[String]): Unit = {

    //TODO 从内存中创建RDD
    val conf = new SparkConf().setMaster("local[*]").setAppName("RddCreate")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,3,5,4,2,6),3)
//glom,把数据合并成分区 ,求分区间最大值的和
    val partition: RDD[Array[Int]] = rdd.glom()
//Array=>max
   val sum = partition.map(array=>array.max).collect.sum
println(sum)

    sc.stop()
   


  }

}
