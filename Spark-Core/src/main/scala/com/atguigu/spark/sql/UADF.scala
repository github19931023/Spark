package com.atguigu.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession, functions}

object UADF {

  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    // TODO 操作数据
    val df: DataFrame = spark.read.json("datas/user.json")

    df.createOrReplaceTempView("user")

    // SparkSQL允许开发人员自定义函数，在SQL中使用
    // 默认的UDF函数是对每一行的数据进行操作
    // 如果想要对所有的行数据进行聚合操作，需要特殊处理

    // UDAF 包含类型的数据操作
    // 但是SQL操作没有类型的概念，如果想要在SQL中使用强类型的UDAF函数，需要特殊处理
    spark.udf.register("avgAge",functions.udaf(new MyAvgAgeUDAF()))

    // 想要在输出的查询结果前增加前缀: Name : zhangsan
    spark.sql("select avgAge(age) from user").show

    spark.stop()
  }
  //样例类：自带伴生对象，可以直接类名使用，这里为例封装使用
  case class Buff( var sum:Long, var cnt:Long )
  /**
   * 用户自定义聚合函数
   * 1. 继承Aggregator类，定义泛型
   *    IN : 聚合函数的输入类型 Long
   *    BUF : buffer(缓冲区)的数据类型 【总的年龄，人数】Buff
   *    OUT : 聚合函数的输出类型 Double
   * 2. 重写方法（4 + 2）
   *
   * Aggregator：强类型
   */
  class MyAvgAgeUDAF extends Aggregator[ Long, Buff, Double ]{
    // 初始化缓冲区
    override def zero: Buff = {
      Buff(0,0)
    }

    // 将输入的年龄和缓冲区的数据进行聚合   分区内计算
    override def reduce(buff: Buff, input: Long): Buff = {
      buff.sum = buff.sum + input
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
