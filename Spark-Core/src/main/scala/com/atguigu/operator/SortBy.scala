package com.atguigu.operator

import org.apache.spark.{SparkConf, SparkContext}

/*
@author 余永蓬
@create 2020-08-04 11:28
*/ object SortBy {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("RddCreate")
    val sc = new SparkContext(conf)

    //TODO Coalesce :缩减分区

   val rdd= sc.makeRDD(List(1,2,3,4,5,6),3)
     //第二个参数表示降序升序
     //字符串默认字典顺序，也可以 .toInt转换成数字排序
     .sortBy(num=>num,false)

    sc.stop()
  }

}
