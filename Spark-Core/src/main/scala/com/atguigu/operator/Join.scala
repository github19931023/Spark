package com.atguigu.operator

import org.apache.spark.{SparkConf, SparkContext}

/*
@author 余永蓬
@create 2020-08-05 10:27
*/ object Join {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("RddCreate")
    val sc = new SparkContext(conf)



    //TODO join：将数据源中相同key的数据进行连接，形成value的元组,不相同的key无法连接
    //TODO value不会聚合
    // rdd1 .join(rdd2)
    //TODO join 会产生笛卡尔积，数据会几何的暴增，性能很低，尽量不用

    //TODO ("a",1) join("a",4) =>(a,(1,4))

    //TODO 还有 leftOuterjoin ：和sql中类似

    //Option类=> Some,None  为了避免空指针

    //TODO cogroup 效果跟 leftOuterjoin 相似，但是先把分区内的相同key的元素
    //TODO 做连接，再分区间作连接
  }

}
