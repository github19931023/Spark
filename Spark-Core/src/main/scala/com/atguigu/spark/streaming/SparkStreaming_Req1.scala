package com.atguigu.spark.streaming


import java.sql.{DriverManager, ResultSet}
import java.text.SimpleDateFormat

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

object SparkStreaming_Req1 {

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

    val adClickSumDS: DStream[((String, String, String), Int)] = adClickDS.transform(
      rdd => {
        // TODO 周期性获取黑名单的数据
        // TODO 读取Mysql中黑名单数据
        val driverClass = "com.mysql.jdbc.Driver"
        val url = "jdbc:mysql://hadoop102:3306/mysql"
        val user = "root"
        val password = "root"
        Class.forName(driverClass)
        val conn = DriverManager.getConnection(url, user, password)
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
    adClickSumDS.foreachRDD(
      rdd => {
        //（（天，用户，广告），sum）
        rdd.foreach{
          case ( (day, userid, adid), sum ) => {
            println(s"处理的数据为：${day} ${userid} ${adid} ${sum}")
            val driverClass = "com.mysql.jdbc.Driver"
            val url = "jdbc:mysql://hadoop102:3306/mysql"
            val user = "root"
            val password = "root"
            Class.forName(driverClass)
            val conn = DriverManager.getConnection(url, user, password)

            if ( sum >= 13 ) {
              // TODO 如果当前统计的结果已经超过阈值，那么直接拉入黑名单

              // （（天，用户，广告1），sum）
              // （（天，用户，广告2），sum）

              val pstat = conn.prepareStatement(
                """
                  |insert into black_list (userid) values (?)
                  |on duplicate key
                  |update userid = ?
                                """.stripMargin)
              pstat.setString(1, userid)
              pstat.setString(2, userid)
              pstat.executeUpdate()
              pstat.close()
            } else {
              // TODO 获取用户当前广告的点击数量
              val pstat1 = conn.prepareStatement(
                """
                  |select
                  |    count
                  |from user_ad_count
                  |where dt = ? and userid = ? and adid = ?
                                """.stripMargin)

              pstat1.setString(1, day)
              pstat1.setString(2, userid)
              pstat1.setString(3, adid)
              val rs = pstat1.executeQuery()
              if ( rs.next() ) {
                // TODO 数据存在的场合
                //      更新用户点击数量
                //      判断更新后的数据有没有超过阈值。
                val oldCount = rs.getInt(1)
                val newCount = oldCount + sum
                if ( newCount >= 13 ) {
                  // TODO   如果超过阈值，那么拉入黑名单
                  val pstat = conn.prepareStatement(
                    """
                      |insert into black_list (userid) values (?)
                      |on duplicate key
                      |update userid = ?
                                        """.stripMargin)
                  pstat.setString(1, userid)
                  pstat.setString(2, userid)
                  pstat.executeUpdate()
                  pstat.close()
                } else {
                  // TODO 更新统计表中的数量
                  val pstat = conn.prepareStatement(
                    """
                      |update user_ad_count
                      |set count = ?
                      |where dt = ? and userid = ? and adid = ?
                                        """.stripMargin)
                  pstat.setInt(1, newCount)
                  pstat.setString(2, day)
                  pstat.setString(3, userid)
                  pstat.setString(4, adid)
                  pstat.executeUpdate()
                  pstat.close()
                }
              } else {
                // TODO 数据不存在的场合
                //      将数据插入到数据库中
                val insertPstat = conn.prepareStatement(
                  """
                    |insert into user_ad_count(dt, userid, adid, count) values (?, ?, ?, ?)
                                    """.stripMargin)
                insertPstat.setString(1, day)
                insertPstat.setString(2, userid)
                insertPstat.setString(3, adid)
                insertPstat.setInt(4, sum)
                insertPstat.executeUpdate()
                insertPstat.close()
              }
              rs.close()
              pstat1.close()
              conn.close()
            }
          }
        }
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }
  case class AdClickData( ts:String, area:String, city:String, userid:String, adid:String )
}
