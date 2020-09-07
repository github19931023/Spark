package com.atguigu.spark.serializable

import org.apache.spark.{SparkConf, SparkContext}

/*
@author 余永蓬
@create 2020-08-05 14:45
*/ object Serializable {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("RddCreate")
    val sc = new SparkContext(conf)


    // TODO Serializable

    val rdd=sc.makeRDD(
      List(1,2,3,4)
    )
     //TODO 从计算的角度, 算子以外的代码都是在Driver端执行, 算子里面的代码都是在Executor端执行，

    //User 为什么要序列化？  因为User类是在Driver端创建的，user的使用却是在Excutor的task执行的
    //这样就需要把User对象通过网络通信传输给Excutor，网络传输需要Serializable
    val user=new User()

    rdd.foreach(
      num=>{
        println("age=" +(user.age+num))

      /*  age=23
        age=24
        age=22
        age=21*/

      }
    )

   sc.stop()   //TODO case:样例类：会自动实现序列化，实现伴生对象的功能 用的方便。


  }

  case class User()  {
    var age=20
  }
}
