package com.atguigu.spark.actions

import org.apache.spark.{SparkConf, SparkContext}

/*
@author 余永蓬
@create 2020-08-05 14:45
*/ object Foreach {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("RddCreate")
    val sc = new SparkContext(conf)


    // TODO  Foreach
    val rdd=sc.makeRDD(
      List(1,2,3,4)
    )
    // TODO collect会将数据按照分区的顺序采集回到driver的内存中再循环打印   顺序打印
    rdd.collect().foreach(println)  // 1,2,3,4  有序

    //分布式打印 在Excutor端执行
    // rdd的方法的代码的声明和执行的位置不太一样
    rdd.foreach(println) // 2,3,1,4 无序

   /*

     TODO  行动算子有哪些：
   reduce count take first collect foreach
      takeOrdered aggregate fold countByKey Save



      */




  }

}
