package com.atguigu.spark.streaming

import java.sql.ResultSet
import java.text.SimpleDateFormat

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

object SparkStreaming18_Req2_AdClick {

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

    // TODO 需求二：广告实时点击量存入Mysql数据库
    val reduceDS: DStream[((String, String, String, String), Int)] = adClickDS.map(
      data => {
        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        ((sdf.format(new java.util.Date(data.ts.toLong)), data.area, data.city, data.adid), 1)
      }
    ).reduceByKey(_ + _)

    // 将结果保存到Mysql中
    reduceDS.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          iter => {
            val conn = JDBCUtil.getConnection
            val pstat = conn.prepareStatement(
              """
                | insert into area_city_ad_count (dt, area, city, adid, count)
                | values ( ?, ?, ?, ?, ?)
                | on duplicate key
                | update count = count + ?
                            """.stripMargin)
            iter.foreach{
              case ( ( day, area, city, adid ), sum ) => {
                pstat.setString(1, day)
                pstat.setString(2, area)
                pstat.setString(3, city)
                pstat.setString(4, adid)
                pstat.setInt(5, sum)
                pstat.setInt(6, sum)
                pstat.executeUpdate()
              }
            }
            pstat.close()
            conn.close()
          }
        )
      }
    )
  ssc.start()
    ssc.awaitTermination()
  }
  case class AdClickData( ts:String, area:String, city:String, userid:String, adid:String )
}
