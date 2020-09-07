package com.atguigu.spark.anli

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Req1_HotCategoryTop10Analysis1 {

    def main(args: Array[String]): Unit = {

        // TODO Top10热门品类
        val sparkConf = new SparkConf().setMaster("local").setAppName("RDDCreate")
        val sc = new SparkContext(sparkConf)

        // TODO 1. 获取原始数据
        val actionRDD = sc.textFile("datas/user_visit_action.txt")

        // TODO 2. 分别统计一个品类的点击数量，下单数量，支付数量

        // 2.1 品类的点击数量的统计
        // 保留点击的数据，其他数据不要
        val clickActionRDD = actionRDD.filter(
            action => {
                val datas: Array[String] = action.split("_")
                datas(6) != "-1"
            }
        )
        // （品类ID, 1 click） => （品类ID, sum click）
        val clickRDD = clickActionRDD.map(
            action => {
                val datas: Array[String] = action.split("_")
                ( datas(6), 1 )
            }
        )

        // （品类ID, sum click）
        val totalClickRDD = clickRDD.reduceByKey(_+_)

        // 2.2 品类的下单数量的统计
        val orderActionRDD = actionRDD.filter(
            action => {
                val datas: Array[String] = action.split("_")
                datas(8) != "null"
            }
        )
        // （品类ID, 1 order） => （品类ID, sum order）
        val orderRDD = orderActionRDD.flatMap(
            action => {
                val datas: Array[String] = action.split("_")
                val categoryids = datas(8)
                val ids = categoryids.split(",")
                ids.map( (_, 1) )
            }
        )

        val totalOrderRDD: RDD[(String, Int)] = orderRDD.reduceByKey(_+_)

        // 2.3 品类的支付数量的统计
        val payActionRDD = actionRDD.filter(
            action => {
                val datas: Array[String] = action.split("_")
                datas(10) != "null"
            }
        )
        // （品类ID, 1 pay） => （品类ID, sum pay）
        val payRDD = payActionRDD.flatMap(
            action => {
                val datas: Array[String] = action.split("_")
                val categoryids = datas(10)
                val ids = categoryids.split(",")
                ids.map( (_, 1) )
            }
        )

        val totalPayRDD: RDD[(String, Int)] = payRDD.reduceByKey(_+_)

        // TODO 3. 将统计结果进行排行(降序)（ 点击， 下单， 支付 ）

        val clickCoOrderRDD: RDD[(String, (Iterable[Int], Iterable[Int]))] = totalClickRDD.cogroup(totalOrderRDD)
        val clickCoOrderMapRDD: RDD[(String, (Int, Int))] = clickCoOrderRDD.map {
            case (id, (left, right)) => {
                (id, (left.head, right.head))
            }
        }
        val clickCoOrderCoPayRDD: RDD[(String, (Iterable[(Int, Int)], Iterable[Int]))] = clickCoOrderMapRDD.cogroup(totalPayRDD)

        val wantRDD = clickCoOrderCoPayRDD.map{
            case ( id, ( left, right ) ) => {
                val (click, order)= left.head
                val pay: Int = right.head
                ( id, (click, order, pay) )
            }
        }

        // TODO 4. 取前10名
        val result: Array[(String, (Int, Int, Int))] = wantRDD.sortBy(_._2, false).take(10)

        result.foreach(println)
        sc.stop

    }
}
