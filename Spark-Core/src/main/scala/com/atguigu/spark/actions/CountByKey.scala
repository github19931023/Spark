package com.atguigu.spark.actions

import org.apache.spark.{SparkConf, SparkContext}

/*
@author 余永蓬
@create 2020-08-05 14:45
*/ object CountByKey {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("RddCreate")
    val sc = new SparkContext(conf)


    // TODO  aggregate aggregateByKey 二者的区别：行动算子。其他功能一样

  /*  val rdd=sc.makeRDD(
      List("hello","spark","hello","scala")
    )
    rdd.map((_,1)).countByKey()  //统计的是key出现的次数*/

    // TODO  countByValue ：这里的value不是kv的v, 而是普通的数值

    val rdd=sc.makeRDD(
      List("hello","spark","hello","scala",1)
    )
    val  rdd2=rdd.countByValue()
    rdd2.foreach(println)

   /* (1,1)
    (hello,2)
    (spark,1)
    (scala,1)*/

  }

}
