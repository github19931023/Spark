package com.atguigu.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
@author 余永蓬
@create 2020-07-31 10:21
*/ object Test03_WordCount {
  def main(args: Array[String]): Unit = {

    //1.建立和Spark的联系
    //创建spark环境对象(上下文对象)，现需要指定环境配置
    val conf=new SparkConf().setMaster("local").setAppName("wordcount")
    val sc=new SparkContext(conf)
    //2.实现操作
    val line: RDD[String] = sc.textFile("D:\\IDEAProjectLocation\\Spark\\Spark-Core\\src\\main\\resources\\wordCount")
    //1 val res=line.flatMap(_.split(" ")).groupBy(word=> word).map(t=> (t._1,t._2.size)).collect().foreach(println)
   // 2 val res=line.flatMap(_.split(" ")).map((_,1)).foldByKey(0)(_+_).collect().foreach(println)
    /* 3 val res=line.flatMap(_.split(" ")).map((_,1))
    res.groupByKey().map{
      case (word,iter)=>{
        (word,iter.sum)
      }
    }*/
  /* 4 val res=line.flatMap(_.split(" ")).map((_,1))
    res.combineByKey(
      num=>num,
      (x:Int,y:Int)=>x+y,
      (x:Int,y:Int)=> x+y
    )
*/

    // 5 val res=line.flatMap(_.split(" ")).map((_,1)).countByKey()

    // 6 val res=line.flatMap(_.split(" ")).map((_,1)).countByValue()

    val res=line.flatMap(_.split(" ")).map((_,1))
   //最终需要得到什么样的结果，就将我们的初始值设为什么样的值







    //3.切断和spark的联系
    sc.stop()


  }


}
