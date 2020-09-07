package com.atguigu.operator

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/*
@author 余永蓬
@create 2020-08-04 11:28
*/ object K_V {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("RddCreate")
    val sc = new SparkContext(conf)


    //TODO k-v类型的数据操作

   val rdd= sc.makeRDD(List(1,2,3,4),2)
    //TODO partitionBy:按指定规则对数据重新分区
    //TODO partitionBy 需要对特定的数据类型(k-v)进行处理

    //TODO RDD中没有 partitionBy方法，但是可以通过隐士类转换规则，转换其他的类，再调用partitionBy
    //TODO 指定规则就是，数据类型为k-v类型

    //TODO partitionBy方法需要传一个分区器对象，对数据分区，默认2个分区器，spark默认用HashPartitoner
    //TODO HashPartitioner底层依靠模运算进行计算分区
    //TODO 数据被打乱，shuffle

    //为什么HashPartitioner底层依靠模运算进行计算分区？
    //要么&运算，要么%运算。 集合容量必须是2的N次方才能用&运算。
    val kvRDD= rdd.map((_,1))
    kvRDD.partitionBy(new HashPartitioner(3))






    sc.stop()
  }

}
