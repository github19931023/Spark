package com.atguigu.spark.anli

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Req1_HotCategoryTop10Analysis3 {

    def main(args: Array[String]): Unit = {

        // TODO Top10热门品类
        val sparkConf = new SparkConf().setMaster("local").setAppName("RDDCreate")
        val sc = new SparkContext(sparkConf)

        // TODO 1. 获取原始数据
        val actionRDD = sc.textFile("datas/user_visit_action.txt")

        // TODO 2. 将原始数据进行结构的转换
        //点击 => ( 品类， （1，0，0） )
        //下单 => ( 品类1，（0，1，0）),  ( 品类2，（0，1，0）)
        //支付 => (品类1，（0，0，1）), (品类2，（0，0，1）)

        // map : 数据不会增多，只是 A => B
        // flatMap : 数据可能会增多

        val flatRDD = actionRDD.flatMap(
            action => {
                val datas = action.split("_")
                if ( datas(6) != "-1" ) {
                    // 点击的场合
                    List( (datas(6), (1, 0, 0)) )
                } else if ( datas(8) != "null" ) {
                    // 下单的场合
                    val ids = datas(8).split(",")
                    ids.map( (_, (0, 1, 0)) )
                } else if ( datas(10) != "null" ) {
                    // 支付的场合
                    val ids = datas(10).split(",")
                    ids.map( (_, (0, 0, 1)) )
                } else {
                    Nil
                }
            }
        )

        val resultRDD = flatRDD.reduceByKey{
            case ( (c1, o1, p1), (c2, o2, p2) ) => {
                ( c1 + c2, o1 + o2, p1 + p2 )
            }
        }
        resultRDD.sortBy(_._2, false).take(10).foreach(println)

        sc.stop

    }
}
