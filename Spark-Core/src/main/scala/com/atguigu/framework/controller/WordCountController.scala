package com.atguigu.framework.controller
import com.atguigu.framework.service.WordCountService
import org.apache.spark.SparkContext

/*
@author 余永蓬
@create 2020-08-10 9:44

调度器对象

*/ class WordCountController {
  private val wordCountService = new WordCountService()


  /**
   * 调度
   * 所谓的调度，就是先执行哪一段逻辑，后执行什么哪一段逻辑
   */

  def dispatch(sc : SparkContext)={
    val result = wordCountService.execute(sc)
    result.foreach(println)

  }

}
