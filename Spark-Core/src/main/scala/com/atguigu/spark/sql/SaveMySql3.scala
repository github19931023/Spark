package com.atguigu.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object SaveMySql3{

  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    // SparkSQL中读取数据时采用SparkSession对象
    //         如果想要保存数据，那么需要采用DataFrame & Dataset

    import spark.implicits._

    val rdd = spark.sparkContext.makeRDD(List(
      (4, "zhangsan", 30),
      (5, "zhangsan", 40),
      (6, "zhangsan", 50)
    ))

    val df = rdd.toDF("id", "name", "age")

    df.write
      .format("jdbc")
      .option("url", "jdbc:mysql://linux1:3306/spark-sql")
      .option("user", "root")
      .option("password", "123123")
      .option("dbtable", "user")
      .mode(SaveMode.Append) // append表示追加数据，追加数据时需要考虑主键是否重复
      .save()
    spark.stop()
  }

}
