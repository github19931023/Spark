package com.atguigu.spark.mock

/*
@author 余永蓬
@create 2020-08-01 9:19

case class Task() :样例类 ，一般用于模式匹配

样例类 可以自动实现可序列化接口 。反序列化不用考虑，流自动实现了

在开发过程中，一般声明Bean类，会声明样例类

*/
case class Task()   {
  var data=List(1,2,3,4)
  var logic:(Int)=>Int=(x:Int)=>{x*2}

  def compute():List[Int]={
    data.map(logic)
  }

}

object Task{
  def main(args: Array[String]): Unit = {

    println(new Task().compute().mkString(","))
  }
}