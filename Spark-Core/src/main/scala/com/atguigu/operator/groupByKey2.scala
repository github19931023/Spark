package com.atguigu.operator

import org.apache.spark.{SparkConf, SparkContext}

/*
@author 余永蓬
@create 2020-08-04 15:35
*/ object groupByKey2 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("RddCreate")
    val sc = new SparkContext(conf)

    val rdd=sc.makeRDD(
      List(("hello",1),("hello",2),("hello",3))
    )

    //groupByKey：效率比较低原因是数据会都落盘，可以采用增加分区数提高效率
    //提高任务的执行的并行度。
    //所有的shuffle操作，都可以通过改变分区数量，适当的提升性能

    rdd.groupByKey(10)
    rdd.distinct(10)
    rdd.reduceByKey(_+_,10)






    //TODO groupByKey 根据key分组，只要key相同
    // 返回结果是 元组类型 (key,value)
     // TODO groupBy 根据指定规则分组



    // TODO groupByKey 和reduceByKey 的区别：前者没有聚合，后者有。都分组
    //TODO reduceByKey 会再shuffle之前进行分区内的聚合叫预先聚合（combine），减少IO次数

    sc.stop()

  }

}
