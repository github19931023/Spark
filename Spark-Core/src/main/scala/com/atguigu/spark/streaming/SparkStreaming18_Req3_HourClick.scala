package com.atguigu.spark.streaming

import java.text.SimpleDateFormat

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

object SparkStreaming18_Req3_HourClick {

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
    // （（时间，广告），1） =》 （（时间，广告），sum）

    // 1分钟， 每10秒统计结果
    // 12:01 000 => 12:00
    // 12:09 => 12:00
    // 12:31 => 12:30
    // 12:59 => 12:50

    // ts => long => 01000 => 00000
    // ts => long => 09000 => 00000
    // ts => long => 31000 => 30000
    // ts => long => 59000 => 50000
    // long / 10000 * 10000
    val mapDS = adClickDS.map(
      data => {
        (( data.ts.toLong / 10000 * 10000, data.adid ), 1)
      }
    )

    // 当窗口范围较大，滑动比较小的场合下，需要使用去重操作。
    val reduceDS = mapDS.reduceByKeyAndWindow(
      (x:Int, y:Int) => x + y,
      //(x:Int, y:Int) => x - y,  有必要就需要去重
      Minutes(1),
      Seconds(10)
    )

    // 将聚合后的结果进行结构的转换
    //  （（10：00， XXX）, 20） => ( XXX, (10:00, 20) )
    //   ((10:00, YYY). 25)      => ( YYY, (10:00, 25))
    // （（10：10， XXX）, 30）  => ( XXX, (10:10, 30))
    // （（时间，广告），sum）=> ( 广告，（时间，sum） )
    val mapDS1 = reduceDS.map{
      case (( ts, ad ), sum) => {
        ( ad, (ts, sum) )
      }
    }

    val result: DStream[(String, Iterable[(Long, Int)])] = mapDS1.groupByKey()

    val sortDS = result.mapValues(
      iter => {
        iter.toList.sortWith(
          (left, right) => {
            left._1.toString < right._1.toString
          }
        )
      }
    )

    sortDS.print()


  ssc.start()
    ssc.awaitTermination()
  }
  case class AdClickData( ts:String, area:String, city:String, userid:String, adid:String )
}
