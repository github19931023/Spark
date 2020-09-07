package com.atguigu.operator

import org.apache.spark.{SparkConf, SparkContext}

/*
@author 余永蓬
@create 2020-08-04 15:35
*/ object ReduceByKey {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("RddCreate")
    val sc = new SparkContext(conf)


    sc.makeRDD(

      List(
        ("hello",1),("hello",2),("hello",3)
      )
    ).reduceByKey(_+_).collect().foreach(println)

    //TODO reduceByKey：将数据源中的每一条数据根据key将value进行聚合
    //TODO 将相同key的数据分组在一起，然后分组后的value进行聚合
    //TODO 他有分组功能，就有shuffle


    // TODO groupByKey 和reduceByKey 的区别：前者没有聚合，后者有。都分组
    //TODO reduceByKey 会再shuffle之前进行分区内的聚合叫预先聚合（combine），减少IO次数


    //TODO RDD中基本所有ByKey的方法都有shuffle操作
    //("hello",1),("hello",2),("hello",3)
    //(hello,(1,2,3))
    //(hello,6)



    sc.stop()

  }

}
