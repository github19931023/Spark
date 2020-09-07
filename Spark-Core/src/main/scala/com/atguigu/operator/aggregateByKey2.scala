package com.atguigu.operator

import org.apache.spark.{SparkConf, SparkContext}

/*
@author 余永蓬
@create 2020-08-04 15:35
*/ object aggregateByKey2 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("RddCreate")
    val sc = new SparkContext(conf)

  //TODO aggregateByKey 也是根据key对value聚合
    // 跟 reduceByKey
    //在某些逻辑中，分区内计算规则和分区间计算规则不相同的，此时reduceByKey 不合适

    //aggregateByKey 在分区内和分区间计算规则不一样的场景可用:max   _+_
    //TODO 去除每个分区内相同key的最大值，然后分区间相加
    //（a,1）(a,5)(b,2)
    //        =>(a,5)(b,2)
    //                 => (a,8)(b,6)
    //        =>（a,3）(b,4)
    //（a,3）(b,4)(b,1)

  val rdd=sc.makeRDD(
    List(
      ("a",1),("a",5),("b",2),
      ("a",3),("b",4),("b",1)
    ),2
  )

    //Scala List.reduce(_+_) => spark RDD.ReduceByKey(_+_)
    //Scala List.fold(_+_) => spark RDD.foldByKey(0)(_+_)

    // fold:是一个集合的元素，和外部的一个元素相加，最终都是求和






    sc.stop()

  }

}
