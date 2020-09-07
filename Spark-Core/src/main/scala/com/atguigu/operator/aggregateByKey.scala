package com.atguigu.operator

import org.apache.spark.{SparkConf, SparkContext}

/*
@author 余永蓬
@create 2020-08-04 15:35
*/ object aggregateByKey {
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

    // 函数柯里化 aggregateByKey（）（）
    // aggregateByKey有2个  参数列表
    // 第一个参数列表传递一个参数
    //    这个参数表示分区内计算的初始值
    // 第二个参数列表传递二个参数
    //   第一个参数表示分区内的计算规则
    //   第二个参数表示分区间的计算规则

    // Scala中的数据计算基本上都是两两计算
    rdd.aggregateByKey(0)(
      (x:Int,y:Int)=> math.max(x,y),
      (x:Int,y:Int)=> x+y
    ).collect().foreach(println) //(b,6) (a,8)




    //TODO groupByKey 根据key分组，只要key相同
    // 返回结果是 元组类型 (key,value)
     // TODO groupBy 根据指定规则分组



    // TODO groupByKey 和reduceByKey 的区别：前者没有聚合，后者有。都分组
    //TODO reduceByKey 会再shuffle之前进行分区内的聚合叫预先聚合（combine），减少IO次数

    sc.stop()

  }

}
