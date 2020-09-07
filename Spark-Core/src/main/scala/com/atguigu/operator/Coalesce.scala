package com.atguigu.operator

import org.apache.spark.{SparkConf, SparkContext}

/*
@author 余永蓬
@create 2020-08-04 11:28
*/ object Coalesce {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("RddCreate")
    val sc = new SparkContext(conf)

    //TODO Coalesce :缩减分区

   val rdd= sc.makeRDD(List(1,2,3,4,5,6),3)
    //TODO 过滤后可能会导致数据急剧减少，那么数据处理的效率低,浪费资源
    //TODO coalesce:所见分区后提高效率
    // 数据不均衡，想要均衡就得使用shuffle，数据没有规律的均衡
    //TODO 第一个：改变分区的数量，如果分区数更大了，没用，因为没shuffle，数据还是按以前的分区放。那么会有分区为空
    //TODO 第二个：在改变时，是否使用shuffle

    //TODO  数据不均衡，想要均衡就得使用shuffle，数据没有规律的均衡

   rdd.coalesce(2,true)

    //TODO 重新分区，底层是coalesce，有shuffle操作，数据会打散。两个方法看情况使用
    rdd.repartition(4)
    sc.stop()
  }

}
