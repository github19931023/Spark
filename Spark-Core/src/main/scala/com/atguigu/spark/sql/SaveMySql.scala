package com.atguigu.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SaveMySql{

  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    // SparkSQL中读取数据时采用SparkSession对象
    //         如果想要保存数据，那么需要采用DataFrame & Dataset
    val json = spark.read.json("datas/user.json")

    // 通用的保存操作（Save）
    // SparkSQL通用保存功能默认的格式为Parquet
    // 如果想要让SparkSQL保存文件中更改数据类型。需要采用特殊的方法
    // 默认保存的路径如果存在的情况下，会发生错误 ： classes-0421/output already exists.
    // 如果在保存路径已经存在情况下，那么想要增加数据，而不是替换数据，那么可以设定数据保存的模式
    json.write.mode("append").format("json").save("output")
    //json.write.mode("error").format("json").save("output")
    //json.write.mode("overwrite").format("json").save("output")
    // json.write.mode("ignore").format("json").save("output")

    spark.stop()
  }

}
