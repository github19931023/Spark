package com.atguigu.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
@author 余永蓬
@create 2020-08-01 14:29
*/ object Test01_Operator_Transform4 {
  def main(args: Array[String]): Unit = {

    //TODO 从内存中创建RDD
    val conf = new SparkConf().setMaster("local[*]").setAppName("RddCreate")
    val sc = new SparkContext(conf)

    val list: List[Int] = List(1,2,3,4,5,6)
    val rdd: RDD[Int] = sc.makeRDD(list,3)
    //将数据和分区号绑定在一起
    //mapPartitionsWithIndex  将数据以分区为单位处理，并且提供分区号
    //两个参数：第一个表示分区索引编号，第二个表示分区数可迭代集合

   /* val mapRDD: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex(
      (index, datas) => {
        datas.map(
          (index, _)
        )
      }
    )*/

    //只对1号分区数据进行操作
/*    rdd.mapPartitionsWithIndex(
      (index,datas)=>{
        if(index==1){
          datas.map(_*4)
        }else{
          datas
        }
      }
    ).collect().foreach(println)*/

    //只保留1号分区数据
   val newRDD= rdd.mapPartitionsWithIndex(
      (index,datas)=>{
        if(index==1){
          datas
        }else{
          Nil.iterator ////匿名函数的返回值类型应该为可迭代的集合
        }
      }
    )


    sc.stop()
   


  }

}
