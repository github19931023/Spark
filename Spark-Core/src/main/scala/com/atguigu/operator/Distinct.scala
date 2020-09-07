package com.atguigu.operator

import org.apache.spark.{SparkConf, SparkContext}

/*
@author 余永蓬
@create 2020-08-04 11:28
*/ object Distinct {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("RddCreate")
    val sc = new SparkContext(conf)

    //TODO 去重
    //scala中是HashSet内存中去重。内存可能不够用
    //spark中是分布式去重
   val rdd= sc.makeRDD(List(1,2,3,2,4,2,3))

    rdd.distinct()
  }

}
