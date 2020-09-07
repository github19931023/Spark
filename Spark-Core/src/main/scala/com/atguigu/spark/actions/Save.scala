package com.atguigu.spark.actions

import org.apache.spark.{SparkConf, SparkContext}

/*
@author 余永蓬
@create 2020-08-05 14:45
*/ object Save {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("RddCreate")
    val sc = new SparkContext(conf)


    // TODO  aggregate aggregateByKey 二者的区别：行动算子。其他功能一样

    val rdd=sc.makeRDD(
      List("hello","spark","hello","scala")
    )
   val rdd2= rdd.map((_,1)).countByKey()


    // 保存成Text文件
    rdd.saveAsTextFile("output")

    // 序列化成对象保存到文件
    rdd.saveAsObjectFile("output1")

    // 保存成Sequencefile文件   顺序文件
    rdd.map((_,1)).saveAsSequenceFile("output2")



  }

}
