package com.atguigu.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/*
@author 余永蓬
@create 2020-08-11 14:25
*/ object SparkSql {
  def main(args: Array[String]): Unit = {

    val conf=new SparkConf().setMaster("local").setAppName("sql")
    //创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //RDD=>DataFrame=>DataSet转换需要引入隐式转换规则，否则无法转换
    //spark不是包名，是上下文环境对象名

    import spark.implicits._

    //读取json文件 创建DataFrame  {"username": "lisi","age": 18}
    val df: DataFrame = spark.read.json("datas/user.json")
    //df.show()

    //SQL风格语法
    df.createOrReplaceTempView("user")
    //spark.sql("select avg(age) from user").show

    //DSL风格语法
    //df.select("username","age").show()

    //*****RDD=>DataFrame=>DataSet*****
    //RDD
    val rdd1: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1,"zhangsan",30),(2,"lisi",28),(3,"wangwu",20)))

    //DataFrame
    val df1: DataFrame = rdd1.toDF("id","name","age")
    //df1.show()

    //DateSet
    val ds1: Dataset[User] = df1.as[User]
    //ds1.show()

    //*****DataSet=>DataFrame=>RDD*****
    //DataFrame
    val df2: DataFrame = ds1.toDF()

    //*****RDD=>DataSet*****
    rdd1.map{
      case (id,name,age)=>User(id,name,age)
    }.toDS().show()



    //*****DataSet=>=>RDD*****
    ds1.rdd

    //释放资源
    spark.stop()


  }

}
case class User(id:Int,name:String,age:Int)