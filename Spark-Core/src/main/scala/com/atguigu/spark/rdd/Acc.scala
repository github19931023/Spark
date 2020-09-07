package com.atguigu.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

/*
@author 余永蓬
@create 2020-08-01 14:29
*/ object Acc {
  def main(args: Array[String]): Unit = {

    //TODO 从内存中创建RDD
    val conf = new SparkConf().setMaster("local").setAppName("Persist")
    val sc = new SparkContext(conf)

/*
    累加器用来把Executor端变量信息聚合到Driver端。在Driver程序中定义的变量，
    在Executor端的每个Task都会得到这个变量的一份新的副本，
    每个task更新这些副本的值后，传回Driver端进行merge。
*/

    val rdd=sc.makeRDD(List(1,2,3,4))
    // TODO 累加器 ：分布式共享只写变量
    //sc.doubleAccumulator()//浮点型
    //sc.collectionAccumulator()//集合类型List

    // 这里的读写概念：不同的累加器（Excutor）之间无法读取的，只能修/改（增加）数据

    var sum= sc.longAccumulator("sum")//整数型
    rdd.foreach(
      num=>{
        sum.add(num)
      }
    )

println("sum="+sum.value)
    sc.stop()


  }

}
