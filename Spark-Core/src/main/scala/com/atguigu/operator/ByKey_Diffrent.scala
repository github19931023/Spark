package com.atguigu.operator

/*
@author 余永蓬
@create 2020-08-05 14:13
*/
import org.apache.spark.{SparkConf, SparkContext}

object ByKey_Diffrent {

  def main(args: Array[String]): Unit = {

    // TODO Spark 转换算子
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDDCreate")
    val sc = new SparkContext(sparkConf)

    // TODO KV类型的数据操作
    val rdd = sc.makeRDD(
      List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98))
    )

    /*

reduceByKey(func)
=> combineByKeyWithClassTag[V]((v: V) => v, func, func)
=> 分区内的第一个key的数据是不参与分区内计算
=> 分区内和分区间的计算规则相同

aggregateByKey（zero）(f1, f2)
=> combineByKeyWithClassTag[U]((v: V) => cleanedSeqOp(createZero(), v), cleanedSeqOp, combOp)
=> 将初始值和分区内第一个key的value进行分区内计算
=> 分区内和分区间的计算规则不一样

foldByKey(zero)(f)
=> combineByKeyWithClassTag[V]((v: V) => cleanedFunc(createZero(), v), cleanedFunc, cleanedFunc)
=> 将初始值和分区内第一个key的value进行分区内计算
=> 分区内和分区间的计算规则相同

combineByKey
=> combineByKeyWithClassTag(createCombiner, mergeValue, mergeCombiners)
=> 分区内每一个key的第一条数据进行转换
=> 分区内和分区间的计算规则不一样

TODO 上面的四个方法都可以在shuffle的map端进行预聚合功能

     */

    rdd.reduceByKey(_+_)
    rdd.aggregateByKey(0)(_+_, _+_)
    rdd.foldByKey(0)(_+_)
    rdd.combineByKey(x=>x,(x:Int, y:Int)=>x+y,(x:Int, y:Int)=>x+y)



    sc.stop()
  }

}
