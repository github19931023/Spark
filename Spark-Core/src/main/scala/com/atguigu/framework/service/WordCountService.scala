package com.atguigu.framework.service

import com.atguigu.framework.dao.WordCountDAO
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/*
@author 余永蓬
@create 2020-08-10 9:48
*/ class WordCountService {
  private val wordCountDAO = new WordCountDAO()

  /**
   * 执行逻辑
   */

  def execute(sc : SparkContext) = {

    val line = wordCountDAO.readFile(sc, "datas/word.txt")
    val words = line.flatMap(_.split(" "))
    val wordToOne = words.map((_, 1))
    val reduce = wordToOne.reduceByKey(_ + _)
    reduce.collect()
  }

}
