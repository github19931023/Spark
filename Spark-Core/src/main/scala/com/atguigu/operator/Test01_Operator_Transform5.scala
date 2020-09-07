package com.atguigu.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
@author 余永蓬
@create 2020-08-01 14:29
*/ object Test01_Operator_Transform5 {
  def main(args: Array[String]): Unit = {

    //TODO 从内存中创建RDD
    val conf = new SparkConf().setMaster("local[*]").setAppName("RddCreate")
    val sc = new SparkContext(conf)

    val tuples= List(List(1, 2), 3, List(4, 5))

    val rdd = sc.makeRDD(tuples)

    //flatMap:扁平化处理 ，返回的参数类型是可迭代的数据类型，比如集合
    //数字 3 怎么处理？
    //让数字3 变成可迭代的集合就行
    rdd.flatMap(
      datas=>{
        datas match {
          case list :List[_]=>list
          case d=> List(d)
        }
      }
    ).collect().foreach(println)




    sc.stop()



  }

}
