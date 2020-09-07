package com.atguigu.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
@author 余永蓬
@create 2020-08-01 14:29
*/ object Test04_RDD_MakeRDD1 {
  def main(args: Array[String]): Unit = {

    //TODO 从内存中创建RDD
    val conf = new SparkConf().setMaster("local[*]").setAppName("RddCreate")
    val sc = new SparkContext(conf)

    val list = List(1, 2, 3, 4,5)
    //parallelize: 可以将集合数据作为数据处理的数据源使用
    //parallelize方法可以创建RDD，并指明RDD中数据的类型
    //parallelize表示并行，但是从代码上不容易理解并行的概念

    //makeRDD第二个参数可以默认不写，写了就是分区数
    val listRDD: RDD[Int] = sc.makeRDD(list,2)
    val listRDD1: RDD[Int] = sc.makeRDD(list,3)
    //将结果保存为分区文件 均匀切分
    listRDD.saveAsTextFile("output1")
    listRDD1.saveAsTextFile("output2")
   // listRDD.collect().foreach(println)

    sc.stop()



  }

}
