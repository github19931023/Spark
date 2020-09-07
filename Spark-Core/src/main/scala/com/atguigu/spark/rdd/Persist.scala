package com.atguigu.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
@author 余永蓬
@create 2020-08-01 14:29
*/ object Persist {
  def main(args: Array[String]): Unit = {

    //TODO 从内存中创建RDD
    val conf = new SparkConf().setMaster("local").setAppName("Persist")
    val sc = new SparkContext(conf)


    val rdd=sc.textFile(" ")

    //TODO cache，persist，checkpoint三者区别：

    // 1 cache/persist 可以提高重复RDD的使用效率，但只能对当前的应用有效
    //2 checkpoint可以提高重复RDD的使用效率，可以跨应用有效
    //3 cache/persist 方法在血缘关系中增加依赖，并不会改变依赖
     //     因为内存不稳定，一旦改变依赖，那么一旦发生错误，数据是无法从头开始执行
    // 4 checkpoint 会切断（改变）血缘关系，会将数据落盘，而且会长久的存储
    //  所以当成新的数据源。那么以前的学院关系就用不上，所以可以切断。
    // checkpoint 和行动算子 不一样，它会独立的再执行一次的作业
    // 因为checkpoint会多次执行相同的作业，为提高效率，可以先缓存操作，再
    // 执行检查点操作，他们是共同存在的

    val rdd2=rdd.flatMap(_.split(" ")).map((_,1))

    rdd2.cache()
    rdd2.checkpoint()

    rdd2.collect().foreach(println)


    sc.stop()


  }

}
