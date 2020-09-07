package com.atguigu.operator

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/*
@author 余永蓬
@create 2020-08-04 15:25
*/ object Self_Partitioner {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("RddCreate")
    val sc = new SparkContext(conf)


    //TODO k-v类型的数据操作

    val rdd= sc.makeRDD(
      List(("nba","消息测试"),
        ("cba","消息测试"),
        ("nba","消息测试"),
        ("nba","消息测试")

      ),2)

   //使用自定义分区器
   val newRDD= rdd.partitionBy(new MyPartitoner(3))
    //给分区绑定索引
   newRDD.mapPartitionsWithIndex(
     (index,datas)=>{
       datas.map(
         data=>{
           (index,data)  //返回
         }
       )
     }
   ).collect().foreach(println)
  /*  (0,(nba,消息测试))
    (0,(nba,消息测试))
    (0,(nba,消息测试))
    (2,(cba,消息测试))*/

    sc.stop()
  }
}

/**
 * 自定义分区器
 * 1. 继承 Partitioner
 * 2. 重写方法
 */

class MyPartitoner(num:Int) extends Partitioner{
  //返回分区的数量
  override def numPartitions: Int = num

  //数据去哪个分区靠这个
  //getPartition ，根据key，返回索引编号，从0开始
  override def getPartition(key: Any): Int = {
    key match {
      case "nba" => 0
      case "wnba"=> 1
      case "cba" =>2
    }
  }
}