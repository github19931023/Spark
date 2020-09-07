package com.atguigu.spark.rdd

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

/*
@author 余永蓬
@create 2020-08-01 14:29
*/ object Broadcast {
  def main(args: Array[String]): Unit = {

    //TODO 从内存中创建RDD
    val conf = new SparkConf().setMaster("local").setAppName("Persist")
    val sc = new SparkContext(conf)


    // TODO 广播变量 ： 分布式共享只读变量
    // 默认情况下，数据的传递是以Task为单位
    // 那么会导致内存中有大量的冗余数据，内存的使用效率变低
    // 如果能够将数据保存在Executor端的内存中，而不是Task的内存
    // 那么只需要保存一份，所有的Task就可以共享这个数据，效率就提高了。
    val list = List("a", "b", "c", "d")
    // TODO 将需要传递的数据进行封装，形成广播变量
    val bc: Broadcast[List[String]] = sc.broadcast(list)

    val rdd = sc.makeRDD(List(
      ("a", 1),("b", 2),("e", 3)
    ))

    val rdd1 = rdd.filter{
      case (w, count) => {
        // TODO 读取广播变量的值
        bc.value.contains(w)
      }
    }

    rdd1.collect().foreach(println)

    sc.stop()


  }

}
