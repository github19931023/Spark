package com.atguigu.operator

import org.apache.spark.{SparkConf, SparkContext}

/*
@author 余永蓬
@create 2020-08-04 15:35
*/ object CombineByKey {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("RddCreate")
    val sc = new SparkContext(conf)

  //TODO CombineByKey 方法可以聚合数据，有一个参数列表，可传3个参数
    // TODO 第一个参数：为例统计方便，先把数据结构转换
    // TODO  第二个：分区内计算规则 => (tuple,y)=>
    //TODO 第三个：分区间计算规则

   /* TODO 小练习：将数据List(("a", 88), ("b", 95), ("a", 91), ("b", 93),
      TODO ("a", 95), ("b", 98))求每个key的平均值

       平均值=total/count



       【("a", 88), ("b", 95), ("a", 91)】
                   => (a, ( 179, 2 )), ( b, (95, 1) )

                                        => ( a, (274, 3) ), (b, ( 286, 3 ))

                    => (a, (95, 1)), (b, (191,2))
         【("b", 93), ("a", 95), ("b", 98)】
       */
   val rdd= sc.makeRDD(List(("a", 88), ("b", 95), ("a", 91), ("b", 93),
       ("a", 95), ("b", 98))

   )
    rdd.combineByKey(
      x=> (x,1),
      (x:(Int,Int), y:Int)=> (x._1 +y,x._2+1),
      (x:(Int,Int), y:(Int,Int)) => ( x._1 + y._1, x._2 + y._2 )


    ).collect.foreach(println)


    sc.stop()

  }

}
