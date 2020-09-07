package com.atguigu.operator

/*
@author 余永蓬
@create 2020-08-04 9:14
*/ object Closer {
  //TODO 闭包演示。。
  //TODO scala中，所有的匿名函数都是闭包
  def main(args: Array[String]): Unit = {

    def outerFun(x:Int)={

      def innerFun(y:Int)={
          x+y
      }

      innerFun _
    }

    val fun= outerFun(5)

    println(fun(10))
  }

}
