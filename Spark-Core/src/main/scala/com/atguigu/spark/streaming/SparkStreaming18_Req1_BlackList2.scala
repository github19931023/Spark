package com.atguigu.spark.streaming

import java.sql.ResultSet
import java.text.SimpleDateFormat

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

object SparkStreaming18_Req1_BlackList2 {

  def main(args: Array[String]): Unit = {

    // TODO SparkStreaming

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount_Streaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val kafkaMap = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "atguigu",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    )
    val kafkaDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](
        Set("atguigu0421"),
        kafkaMap
      )
    )

    // TODO 获取用户点击广告的数据
    //val dataDS: DStream[String] = kafkaDS.map(_.value())
    val adClickDS: DStream[AdClickData] = kafkaDS.map(
      data => {
        val datas = data.value().split(" ")
        AdClickData(datas(0), datas(1), datas(2), datas(3), datas(4))
      }
    )

    // TODO 业务中需要频繁地范围Mysql数据库
    // 那么减少和数据量的连接是可以提高性能
    // 这里采用数据库连接池

    val adClickSumDS: DStream[((String, String, String), Int)] = adClickDS.transform(
      rdd => {
        // TODO 周期性获取黑名单的数据
        // TODO 读取Mysql中黑名单数据
        val conn = JDBCUtil.getConnection
        val pstat = conn.prepareStatement("select userid from black_list")
        val rs: ResultSet = pstat.executeQuery()
        // 黑名单数据
        val blackList = ListBuffer[String]()
        while (rs.next()) {
          blackList.append(rs.getString(1))
        }
        rs.close()
        pstat.close()
        conn.close()

        // TODO 对采集的数据进行过滤
        val filterRDD = rdd.filter(
          data => {
            !blackList.contains(data.userid)
          }
        )

        // TODo 将正常访问的数据进行统计
        // （（天，用户，广告），1） => （（天，用户，广告），sum）
        // TODO 将黑名单数据中的用户数据过滤掉，不进行统计
        //      只对正常广告的点击数据进行统计
        //      (（天， 用户， 广告）， clickCount)
        filterRDD.map(
          data => {
            // ts => date
            val sdf = new SimpleDateFormat("yyyy-MM-dd")
            var dateString = sdf.format(new java.util.Date(data.ts.toLong))
            ((dateString, data.userid, data.adid), 1)
          }
        ).reduceByKey(_ + _)
      }
    )

    // TODO 更新Mysql中用户广告点击数据
    // 将当前的数据和今天历史数据进行合并
    // 减少数据库的连接数量，可以提高效率

    //

    adClickSumDS.foreachRDD(
      rdd => {
        //（（天，用户，广告），sum）

        // 所谓的算子，其实就是RDD的方法
        // foreach算子
        // 算子的外部代码在driver端执行的
        // 算子的内部代码在executor端执行的。
        // 数据库连接对象应该通过闭包从driver端传递到executor端，所以需要序列化
        // 但是，数据库连接对象不允许序列化。所以，连接对象没有办法通过闭包传递
        // Spark提供了性能较高的算子 ： foreachPartition
        // foreachPartition 和 foreach的关系类似于 map 和 mapPartitions的关系
        rdd.foreachPartition(
          iter => {

            val conn = JDBCUtil.getConnection
            // 更新用户点击统计表
            val insertPstat = conn.prepareStatement(
              """
                |insert into user_ad_count(dt, userid, adid, count)
                |values (?, ?, ?, ?)
                |on duplicate key
                |update count = count + ?
                            """.stripMargin)

            // 判断是否超过阈值
            val selectPstat = conn.prepareStatement(
              """
                |select
                |    userid
                |from user_ad_count
                |where dt = ? and userid = ? and adid = ? and count >= 20
                            """.stripMargin)

            // 加入黑名单
            val blackListPstat = conn.prepareStatement(
              """
                |insert into black_list (userid) values (?)
                |on duplicate key
                |update userid = ?
                            """.stripMargin)

            iter.foreach{
              case ( (day, userid, adid), sum ) => {

                println(s"处理的数据为：${day} ${userid} ${adid} ${sum}")
                // TODO 更新统计表的数据
                insertPstat.setString(1, day)
                insertPstat.setString(2, userid)
                insertPstat.setString(3, adid)
                insertPstat.setInt(4, sum)
                insertPstat.setInt(5, sum)
                insertPstat.executeUpdate()

                // TODO 查询超过阈值的数据
                selectPstat.setString(1, day)
                selectPstat.setString(2, userid)
                selectPstat.setString(3, adid)
                val rs = selectPstat.executeQuery()

                if ( rs.next() ) {
                  // TODO 插入黑名单
                  blackListPstat.setString(1, userid)
                  blackListPstat.setString(2, userid)
                  blackListPstat.executeUpdate()
                }
                rs.close()

              }

                insertPstat.close()
                selectPstat.close()
                blackListPstat.close()

                conn.close()
            }
          }
        )
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }
  case class AdClickData( ts:String, area:String, city:String, userid:String, adid:String )
}
