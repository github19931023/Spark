package com.atguigu.framework.dao

import org.apache.spark.SparkContext
/*
@author 余永蓬
@create 2020-08-10 9:47
*/ class WordCountDAO {
  def readFile( sc : SparkContext, path:String ) = {
    sc.textFile(path)
  }

}
