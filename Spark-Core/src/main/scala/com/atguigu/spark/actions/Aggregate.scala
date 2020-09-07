package com.atguigu.spark.actions

import org.apache.spark.{SparkConf, SparkContext}

/*
@author 余永蓬
@create 2020-08-05 14:45
*/ object Aggregate {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("RddCreate")
    val sc = new SparkContext(conf)


    // TODO  aggregate aggregateByKey 二者的区别：行动算子。其他功能一样

    val rdd=sc.makeRDD(
      List(1,2,3,4),2
    ).aggregate(10)(_+_,_+_)  //40

    // (1,2)=>(11,2)=>13
    //(3,4)=>(13,4)=17     aggregateByKey

    // (1,2)=>(11,2)=>13
    //(3,4)=>(13,4)=17    aggregate  初始值10 也会在分区间相加。10+30=40

    // fold =aggregate  ：在区分内，分区间规则相同的时候

  }

}
