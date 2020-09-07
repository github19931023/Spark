package com.atguigu.spark.anli

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Req1_HotCategoryTop10Analysis2 {

    def main(args: Array[String]): Unit = {

        // TODO Top10热门品类
        val sparkConf = new SparkConf().setMaster("local").setAppName("RDDCreate")
        val sc = new SparkContext(sparkConf)

        // TODO 1. 获取原始数据
        val actionRDD = sc.textFile("datas/user_visit_action.txt")
        actionRDD.cache()

        // TODO 2. 分别统计一个品类的点击数量，下单数量，支付数量

        // Q : actionRDD，读取文件中，重复使用?
        // A : 即使对象重复使用，但是数据无法重复使用，因为RDD不保存数据，从头执行数据操作
        //     可以采用持久化操作
        // Q : reduceByKey算子被调用的次数过多，性能下降。？
        // A : 极限情况下，使用一个reduceByKey实现功能
        // Q : 连接3个数据源时，会导致性能下降？
        // A : 改变数据结构，提升计算性能

        val clickActionRDD = actionRDD.filter(
            action => {
                val datas: Array[String] = action.split("_")
                datas(6) != "-1"
            }
        )

        val clickRDD = clickActionRDD.map(
            action => {
                val datas: Array[String] = action.split("_")
                ( datas(6), 1 )
            }
        )
        // （品类ID，1）=> ( 品类，（1，0，0） )
        // reduceByKey丢弃？？？
        val totalClickRDD = clickRDD.reduceByKey(_+_)

        val orderActionRDD = actionRDD.filter(
            action => {
                val datas: Array[String] = action.split("_")
                datas(8) != "null"
            }
        )

        val orderRDD = orderActionRDD.flatMap(
            action => {
                val datas: Array[String] = action.split("_")
                val categoryids = datas(8)
                val ids = categoryids.split(",")
                ids.map( (_, 1) )
            }
        )

        // （品类ID，1） => （品类，（0，1，0））
        // reduceByKey是否丢弃？？？
        val totalOrderRDD: RDD[(String, Int)] = orderRDD.reduceByKey(_+_)

        val payActionRDD = actionRDD.filter(
            action => {
                val datas: Array[String] = action.split("_")
                datas(10) != "null"
            }
        )

        val payRDD = payActionRDD.flatMap(
            action => {
                val datas: Array[String] = action.split("_")
                val categoryids = datas(10)
                val ids = categoryids.split(",")
                ids.map( (_, 1) )
            }
        )

        // （品类ID， 1）=> (品类，（0，0，1）)
        // reduceByKey丢弃？？？
        val totalPayRDD: RDD[(String, Int)] = payRDD.reduceByKey(_+_)

        // TODO 3. 将统计结果进行排行(降序)（ 点击， 下单， 支付 ）

        // ( 品类ID， (点击数量, 0, 0) )
        val newClickRDD = totalClickRDD.map{
            case ( id, clickCnt ) => {
                ( id, (clickCnt, 0, 0) )
            }
        }
        // ( 品类ID， (0, 下单数量, 0) )
        val newOrderRDD = totalOrderRDD.map {
            case ( id, orderCnt ) => {
                ( id, (0, orderCnt, 0) )
            }
        }
        // ( 品类ID， (0, 0, 支付数量) )
        val newPayRDD = totalPayRDD.map {
            case ( id, payCnt ) => {
                ( id, (0, 0, payCnt) )
            }
        }
        // (1, (1,0,0))
        // (1, (0,2,0))
        // (1, (0,0,3))
        // => (1, (1,2,3))
        // union
        val rdd: RDD[(String, (Int, Int, Int))] = newClickRDD.union(newOrderRDD).union(newPayRDD)
        // ( 品类ID, ( 点击数量，下单数量，支付数量 ) )
        val wantRDD = rdd.reduceByKey{
            case ( (c1, o1, p1), (c2, o2, p2) ) => {
                ( c1 + c2, o1 + o2, p1 + p2 )
            }
        }
        // TODO 4. 取前10名
        val result: Array[(String, (Int, Int, Int))] = wantRDD.sortBy(_._2, false).take(10)

        result.foreach(println)
        sc.stop

    }
}
