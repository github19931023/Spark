package com.atguigu.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkSql_Hive{

  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    // SparkSQL中读取数据时采用SparkSession对象
    //         如果想要保存数据，那么需要采用DataFrame & Dataset
    // SparkSQL连接Hive


    import spark.implicits._

    val rdd = spark.sparkContext.makeRDD(List(
     2,3,4
    ))

    val df = rdd.toDF("id")

    df.write
      .format("jdbc")
      .option("url", "jdbc:mysql://hadoop102:3306/mysql")
      .option("user", "root")
      .option("password", "root")
      .option("dbtable", "user2")
      .mode(SaveMode.Append) // append表示追加数据，追加数据时需要考虑主键是否重复
      .save()
    spark.stop()
  }

}
