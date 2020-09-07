package com.atguigu.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkSql_Hive3{

  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()
    // SparkSQL中读取数据时采用SparkSession对象
    //         如果想要保存数据，那么需要采用DataFrame & Dataset
    // SparkSQL连接Hive

    // TODO 1. 增加依赖
    // TODO 2. 在创建SparkSession环境对象时，应该启用Hive的支持:enableHiveSupport
    // TODO 3. 将Hive的配置文件hive-site.xml复制到项目的resources目录中
    //         IDEA在运行时会从classpath中查找配置文件，项目的classpath指向的是项目的target/classes目录

    spark.sql("show tables").show


    spark.stop()
  }

}
