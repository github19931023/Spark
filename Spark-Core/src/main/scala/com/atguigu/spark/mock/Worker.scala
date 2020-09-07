package com.atguigu.spark.mock

import java.io.ObjectInputStream
import java.net.ServerSocket

/*
@author 余永蓬
@create 2020-08-01 8:51
*/ object Worker {
  def main(args: Array[String]): Unit = {
  //TODO 执行计算的节点
    //建立服务器，接收master的传递任务
    val server=new ServerSocket(8888)

    println("worker已经启动")
    val master=server.accept()

    //接收任务
    val objIn=new ObjectInputStream(master.getInputStream)
    val task=objIn.readObject().asInstanceOf[Task]

    println("计算结果："+task.compute().mkString(","))
    master.close()
    server.close()


  }

}
