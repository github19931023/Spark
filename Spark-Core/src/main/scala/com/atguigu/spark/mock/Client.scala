package com.atguigu.spark.mock

import java.io.{OutputStreamWriter, PrintWriter}
import java.net.Socket


/*
@author 余永蓬
@create 2020-08-01 8:51
*/ object Client {
  def main(args: Array[String]): Unit = {

    //TODo 客户端，将计算发送集群处理
    val master=new Socket("localhost",9999)
    println("已经连上，可以发送。。。")

    //发送字符串指令
  /*  val out=master.getOutputStream
    out.write(1)
    out.flush()*/
  //发送字符串指令PrintWriter
    //master.getOutputStream 字节流
    //字节流=>字符流
    val writer=new PrintWriter(
    new OutputStreamWriter(master.getOutputStream,"UTF-8")
  )
    //windos下打开记事本
    writer.println("CMD /c notePad")
    writer.flush()
    println("客户端发送的指令字符串成功")



    master.close()

  }

}
