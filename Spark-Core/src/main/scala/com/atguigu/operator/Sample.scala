package com.atguigu.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
@author 余永蓬
@create 2020-08-04 10:35
*/ object Sample {
  def main(args: Array[String]): Unit = {

    //TODO 过滤 分区不变， 分区数据可能不均衡，生产中可能会数据倾斜
    val conf = new SparkConf().setMaster("local[*]").setAppName("RddCreate")
    val sc = new SparkContext(conf)

    //TODO 采样(抽取数据)：sample
    //抽取数据分两种情况：
    //1,抽取完后会放回数据，再重新抽
    //2，抽取后，不会放回
    val rdd=sc.makeRDD(List(1,2,3,4,5,6))
    //sample3个参数：
    //1，表示抽取的数会不会放回，true，false
    //2，在不放回的情况下，表示抽取每一条数据的概率
    //          如果放回去，那么表示的是数据可能被抽取的次数
    //3,表示随机数的种子,类似于打分，如果种子不变，打分的分值不变
    val sampleRDD = rdd.sample(false,0.5,100)

    println(sampleRDD.collect().mkString(","))
    sc.stop()
  }

}
