package com.atguigu.framework.commom
import com.atguigu.framework.controller.WordCountController
import org.apache.spark.{SparkConf, SparkContext}
/*
@author 余永蓬
@create 2020-08-10 9:42

Scala中的特质其实就是抽象类和接口的结合体
*/ trait TApplication {

  //函数柯里化
  //控制抽象
  def runApplication(master:String = "local[*]", appName:String = "Application")()={
    val conf: SparkConf = new SparkConf().setMaster(master).setAppName(appName)
    val sc : SparkContext = new SparkContext(conf)

    //TODO 执行控制器代码
    val controller = new WordCountController()
    controller.dispatch(sc)
    

    sc.stop()

  }

}
