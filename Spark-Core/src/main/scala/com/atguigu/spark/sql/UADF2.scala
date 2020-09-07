package com.atguigu.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._

object UADF2 {   //  DLS语法：方法对象

  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
   import spark.implicits._
    // TODO 操作数据
    val df: DataFrame = spark.read.json("datas/user.json")


    // UDAF(强类型) + DSL语法
    val ds: Dataset[User] = df.as[User]

    //select 方法也可以传递字符串的类名
    //也可以传递聚合函数转换的列对象

    val udaf = new MyAvgAgeUDAF()
    ds.select(udaf.toColumn).show()




    spark.stop()
  }
  case class User(id:Long,name: String,age:Long)
  //样例类：自带伴生对象，可以直接类名使用，这里为例封装使用
  case class Buff( var sum:Long, var cnt:Long )
  /**
   * 用户自定义聚合函数
   * 1. 继承Aggregator类，定义泛型
   *    IN : 聚合函数的输入类型 User
   *    BUF : buffer(缓冲区)的数据类型 【总的年龄，人数】Buff
   *    OUT : 聚合函数的输出类型 Double
   * 2. 重写方法（4 + 2）
   *
   * Aggregator：强类型
   */
  class MyAvgAgeUDAF extends Aggregator[ User, Buff, Double ]{
    // 初始化缓冲区
    override def zero: Buff = {
      Buff(0,0)
    }

    // 将输入的年龄和缓冲区的数据进行聚合   分区内计算
    override def reduce(buff: Buff, input: User): Buff = {
      buff.sum = buff.sum + input.age
      buff.cnt = buff.cnt + 1
      buff
    }

    // 多个缓冲区数据合并   分区间计算
    override def merge(buff1: Buff, buff2: Buff): Buff = {
      buff1.sum = buff1.sum + buff2.sum
      buff1.cnt = buff1.cnt + buff2.cnt
      buff1
    }

    // 完成聚合操作，获取最终结果
    override def finish(buff: Buff): Double = {
      buff.sum.toDouble / buff.cnt
    }

    // SparkSQL对传递的对象的序列化操作（编码）  这两个方法固定写法
    override def bufferEncoder: Encoder[Buff] = Encoders.product
    override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
  }

}
