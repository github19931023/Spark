package com.atguigu.spark.anli

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Req1_HotCategoryTop10Analysis4 {

    def main(args: Array[String]): Unit = {

        // TODO Top10热门品类
        val sparkConf = new SparkConf().setMaster("local").setAppName("RDDCreate")
        val sc = new SparkContext(sparkConf)

        // TODO 1. 获取原始数据
        val actionRDD = sc.textFile("datas/user_visit_action.txt")

        // TODO 不使用shuffle操作就能完成数据的统计
        // 累加器
        val acc = new HotCategoryAccumulator()
        sc.register(acc, "hotCategory")

        actionRDD.foreach(
            action => {
                val datas = action.split("_")
                if ( datas(6) != "-1" ) {
                    // 点击的场合
                    acc.add( (datas(6), "click") )
                } else if ( datas(8) != "null" ) {
                    // 下单的场合
                    val ids = datas(8).split(",")
                    ids.foreach(
                        id => {
                            acc.add( (id, "order") )
                        }
                    )
                } else if ( datas(10) != "null"  ) {
                    // 支付的场合
                    val ids = datas(10).split(",")
                    ids.foreach(
                        id => {
                            acc.add( (id, "pay") )
                        }
                    )
                }
            }
        )

        val hotCategoryMap: mutable.Map[String, HotCategory] = acc.value

        // TODO 将累加的结果进行排序(降序)和取前10名
        hotCategoryMap.map(_._2).toList.sortWith(
            (left, right) => {
                if ( left.clickCount > right.clickCount ) {
                    true
                } else if ( left.clickCount == right.clickCount ) {
                    if ( left.orderCount > right.orderCount ) {
                        true
                    } else if ( left.orderCount == right.orderCount ) {
                        left.payCount > right.payCount
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
        ).take(10).foreach(println)

        sc.stop

    }
    // IN : (品类，行为类型)
    // OUT : Map[品类，HotCategory]
    class HotCategoryAccumulator extends AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]{

        private val hotCategoryMap = mutable.Map[String, HotCategory]()

        override def isZero: Boolean = {
            hotCategoryMap.isEmpty
        }

        override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = {
            new HotCategoryAccumulator()
        }

        override def reset(): Unit = {
            hotCategoryMap.clear()
        }

        override def add(t: (String, String)): Unit = {
            val id = t._1
            val actionType = t._2
            val hotCategory = hotCategoryMap.getOrElse(id, HotCategory(id, 0, 0, 0))

            actionType match {
                case "click" => hotCategory.clickCount += 1
                case "order" => hotCategory.orderCount += 1
                case "pay" => hotCategory.payCount += 1
            }

            hotCategoryMap.update( id, hotCategory )
        }

        override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {

            other.value.foreach{
                case (id, otherHC) => {
                    val thisHC = hotCategoryMap.getOrElse(id, HotCategory(id, 0, 0, 0))

                    thisHC.clickCount += otherHC.clickCount
                    thisHC.orderCount += otherHC.orderCount
                    thisHC.payCount += otherHC.payCount

                    hotCategoryMap.update(id, thisHC)
                }
            }

        }

        override def value: mutable.Map[String, HotCategory] = hotCategoryMap
    }
    case class HotCategory(
          var categoryId : String,
          var clickCount : Long,
          var orderCount : Long,
          var payCount : Long,
      )
}
