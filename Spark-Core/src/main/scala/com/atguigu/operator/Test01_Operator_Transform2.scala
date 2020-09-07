package com.atguigu.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
@author 余永蓬
@create 2020-08-01 14:29
*/ object Test01_Operator_Transform2 {
  def main(args: Array[String]): Unit = {

    //TODO 从内存中创建RDD
    val conf = new SparkConf().setMaster("local[*]").setAppName("RddCreate")
    val sc = new SparkContext(conf)

    val list: List[Int] = List(1,2,3,4)
    val rdd1: RDD[Int] = sc.makeRDD(list,2)
    val rdd2: RDD[Int] = rdd1.mapPartitions(
      partRdd => {
        partRdd.map(_ * 2)
      }
    )
    rdd2.collect().foreach(println)




    sc.stop()
   


  }

}
