package com.atguigu.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SaveMySql2{

  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    // SparkSQL中读取数据时采用SparkSession对象
    //         如果想要保存数据，那么需要采用DataFrame & Dataset
    import spark.implicits._

    // CSV
    //        spark.read.format("csv")
    //                .option("sep", ";")
    //                .option("inferSchema", "true")
    //                .option("header", "true")
    //                .load("datas/people.csv").show

   //  MySQL
            spark.read.format("jdbc")
                    .option("url", "jdbc:mysql://hadoop102:3306/mysql")
                    .option("driver", "com.mysql.jdbc.Driver")
                    .option("user", "root")
                    .option("password", "root")
                    .option("dbtable", "user2")
                    .load().show
    spark.stop()
  }

}
