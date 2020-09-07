package com.atguigu.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Req1{

  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

    spark.sql("use default")

 spark.sql(
   """
     |select
     |    *
     |from (
     |    select
     |        *,
     |        rank() over( partition by area order by clickCount desc ) as rank
     |    from (
     |        select
     |            area,
     |            product_name,
     |            count(*) as clickCount
     |        from (
     |            select
     |                a.*,
     |                c.area,
     |                c.city_name,
     |                p.product_name
     |            from user_visit_action a
     |            join city_info c on a.city_id = c.city_id
     |            join product_info p on a.click_product_id = p.product_id
     |            where a.click_product_id > -1
     |        ) t1 group by area, product_name
     |    ) t2
     |) t3
     |where rank <= 3
     |""".stripMargin).show()

    spark.stop()
  }

}
