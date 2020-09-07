package com.atguigu.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
@author 余永蓬
@create 2020-08-01 14:29
*/ object Test01_Operator_Transform7 {
  def main(args: Array[String]): Unit = {

    //TODO 从内存中创建RDD
    val conf = new SparkConf().setMaster("local[*]").setAppName("RddCreate")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))
    val groupRDD: RDD[(Int, Iterable[Int])] = rdd.groupBy(num => num % 2)
    //TODO groupBy:会将原数据打乱，组成新的分区数据，分区数量没变
   groupRDD.collect.foreach(println)


    sc.stop()
   


  }

}
