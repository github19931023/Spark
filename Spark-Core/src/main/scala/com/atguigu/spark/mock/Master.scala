package com.atguigu.spark.mock

import java.io.{BufferedReader, InputStreamReader, ObjectOutputStream}
import java.net.{ServerSocket, Socket}

/*
@author 余永蓬
@create 2020-08-01 8:51
*/ object Master {
  def main(args: Array[String]): Unit = {

    //TODO 集群管理器，起到调度作用
    //启动服务器，接收客户端请求
    /*val server=new ServerSocket(9999)
    //accept
    val client=server.accept()

    //接收数据 字节流
   /* val in= client.getInputStream
    val i=in.read()
    println("服务器的数据："+i)*/

    //字符流
    val reader=new BufferedReader(
      new InputStreamReader(
        client.getInputStream,"UTF-8"
      )
    )
    //获取客户端发送的指令字符串
    val command=reader.readLine()
    println("客户端发送的指令字符串:"+command)

    //执行命令
    Runtime.getRuntime.exec(command)

    println("执行成功。。。")
    client.close()
    server.close()*/

    //连接worker，让worker计算
    val worker=new Socket("localhost",8888)
    println("连接到worker节点")

    //发送数据为计算任务
    val task=new Task()

    //输出流发送对象
    //

    val se=new ObjectOutputStream(worker.getOutputStream)

     se.writeObject(task)
    se.flush()

    worker.close()
  }

}
