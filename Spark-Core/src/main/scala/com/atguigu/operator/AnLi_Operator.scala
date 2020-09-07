package com.atguigu.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
@author 余永蓬
@create 2020-08-05 11:13
*/ object AnLi_Operator {
  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setMaster("local[*]").setAppName("RddCreate")
    val sc = new SparkContext(conf)

    //TODO 1，从文件获取原始数据
    val logrdd=sc.textFile("D:\\IDEAProjectLocation\\Spark\\Spark-Core\\src\\main\\resources\\agent.log")


    //TODO 2，为统计方便，数据结构转换 map
    // line => ( 省份， 广告 )=>（（省份，广告），1）
    val rdd1 = logrdd.map(
      log => {
        val data = log.split(" ")
        ((data(1), data(4)), 1)
      }


    )


    //TODO 3，将转换结构后的数据进行分组聚合，reduceByKey
    // （（省份，广告），1）=> （（省份，广告），sum）
   val rdd2= rdd1.reduceByKey(_+_)



    //TODO 4,将聚合的结果进行结构转换
    // （（省份，广告），sum）=> （省份，（广告，sum））

    val mapRDD=rdd2.map{
      case ((prv,ad),sum)=>{
        (prv,(ad,sum))
      }
    }


    //TODO 5，将进行结构转换的数据按省份分组
    // （省份，（广告，sum））=>(省份，Iterator[])
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD.groupByKey()


    //TODO 6，将分组后的数据，根据广告的点击量进行排序（降序），取前3名
    val res=groupRDD.map{
      case (prv,iter)=>{
        // 对分组后的数据进行排序
        val resList=iter.toList.sortWith(
          (_._2 >_._2)
        ).take(3)

        (prv,resList)

      }
    }
    res.collect().foreach(println)


    sc.stop()

  }

}
