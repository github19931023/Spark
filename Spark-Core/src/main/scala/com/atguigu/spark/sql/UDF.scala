package com.atguigu.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object UDF{

  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    // TODO 操作数据
    val df: DataFrame = spark.read.json("datas/user.json")

    df.createOrReplaceTempView("user")

    // SparkSQL允许开发人员自定义函数，在SQL中使用
    spark.udf.register("addPrefixName",(name:String)=>{"Name:" + name})

    // 想要在输出的查询结果前增加前缀: Name : zhangsan
    spark.sql("select addPrefixName(name) from user").show

    spark.stop()
  }

}
