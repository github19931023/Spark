package com.atguigu.spark.sql
import org.apache.parquet.schema.Types.ListBuilder
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Req2{

  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

    spark.sql("use default")
    spark.sql(
      """
        |select
        |                a.*,
        |                c.area,
        |                c.city_name,
        |                p.product_name
        |            from user_visit_action a
        |            join city_info c on a.city_id = c.city_id
        |            join product_info p on a.click_product_id = p.product_id
        |            where a.click_product_id > -1
        |""".stripMargin).createOrReplaceTempView("t1")

    val cityRemarkUDAF=new CityRemarkUDAF()
    spark.udf.register("cityRemark",functions.udaf(cityRemarkUDAF))
    spark.sql(
      """
        |select
        |            area,
        |            product_name,
        |            count(*) as clickCount,
        |            cityRemark(city_name) as city_remarkxxxxxxxxxxxxxxxxx
        |        from
        |
        |         t1 group by area, product_name
        |""".stripMargin).createOrReplaceTempView("t2")

    spark.sql(
      """
        |select
        |        *,
        |        rank() over( partition by area order by clickCount desc ) as rank
        |    from t2
        |""".stripMargin).createOrReplaceTempView("t3")

    spark.sql(
      """
        |select
        |    *
        |from
        |
        |
        |
        | t3
        |where rank <= 3
        |""".stripMargin).show()



    spark.stop()
  }

  //城市备注需要聚合计算的
  case class Buffer(var totalcnt:Long,var cityMap:mutable.Map[String,Long])
  /*
  城市备注聚合函数
  1.继承Aggregator，定义泛型
    IN：城市名称 String
    BUFF: 缓冲区。每个城市点击数量Map[ （cityName，点击数量） ]/ 所以城市的点击数量（totalcnt）
    OUT:城市备注 String
    2.重写方法（4+2）
   */
  class CityRemarkUDAF extends Aggregator[String, Buffer, String] {
    override def zero: Buffer = {
      Buffer(0L,mutable.Map[String,Long]())

    }
//更新缓冲区 ，总数+1
    override def reduce(buffer: Buffer, city: String): Buffer = {
      buffer.totalcnt+=1
      val newCount=buffer.cityMap.getOrElse(city,0L)+1
      //更新缓冲区 有点像flush，来一个更新一下
      buffer.cityMap.update(city,newCount)
      buffer //返回

    }
    // 合并缓冲区的数据
    override def merge(b1: Buffer, b2: Buffer): Buffer = {

      //合并所以城市的点击数量总合
      b1.totalcnt+=b2.totalcnt
      //再合并城市Map（2个Map的合并）
      val map1= b1.cityMap
      val map2=b2.cityMap
      b1.cityMap= map1.foldLeft(map2){
        case (map,(city,cnt)) => {
          val newCnt=map1.getOrElse(city,0L)+cnt
          map1.update(city,newCnt)
          map
        }
      }

    b1
    }
    // 计算结果：字符串（每个城市的点击比率）ListBuffer集合中逗号隔开
    override def finish(buffer: Buffer): String = {
      val remarkList=ListBuffer[String]()

      val totalcnt=buffer.totalcnt
      val cityMap = buffer.cityMap

      //将统计城市点击数量的集合进行排序
     val cityCountList =buffer.cityMap.toList.sortWith(
        (left,Right)=>{
          left._2 >Right._2
        }
      ).take(2)
      // 判断是否存在其他的城市
      val hasOtherCity = cityMap.size > 2
      var sum = 0L  //前面2个城市比重总和
      //往集合里放数据
      cityCountList.foreach{
        case (city,cnt)=>{
          val r = cnt * 100 / totalcnt
          remarkList.append(city+" "+r +"%")
          sum += r
        }
      }
      if(hasOtherCity){
        remarkList.append("其他 " + (100 - sum) + "%" )
      }
      remarkList.mkString(",")
    }

    override def bufferEncoder: Encoder[Buffer] = {
      Encoders.product  //固定写法
    }

    override def outputEncoder: Encoder[String] = {
      Encoders.STRING //固定写法
    }
  }

}
