package com.atguigu.operator

import org.apache.spark.{SparkConf, SparkContext}

/*
@author 余永蓬
@create 2020-08-04 11:28
*/ object TwoValue {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("RddCreate")
    val sc = new SparkContext(conf)

    //TODO 两个value型的值，就是集合
    //TODO 两个集合类型必须一致，不然报错

   val rdd= sc.makeRDD(List(1,2,3,4,5,6))
    val rdd1= sc.makeRDD(List(1,2,3,4))

    //TODO 交集
    rdd.intersection(rdd1)
    //TODO 并集
    rdd.union(rdd1)
    //TODO 差集  以rdd为准把交叉的去掉
    val rdd2=rdd.subtract(rdd1)  //5,6
    println(rdd2.collect().mkString(","))



    sc.stop()
  }

}
