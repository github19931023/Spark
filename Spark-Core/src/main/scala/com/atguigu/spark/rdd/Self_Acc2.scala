package com.atguigu.spark.rdd

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

/*
@author 余永蓬
@create 2020-08-01 14:29
*/ object Self_Acc2 {  // TODO 求平均值
  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setMaster("local").setAppName("Persist")
    val sc = new SparkContext(conf)

/*
    累加器用来把Executor端变量信息聚合到Driver端。在Driver程序中定义的变量，
    在Executor端的每个Task都会得到这个变量的一份新的副本，
    每个task更新这些副本的值后，传回Driver端进行merge。
*/

    val rdd=sc.makeRDD(List(1,2,3,4,5,6))
    // TODO 累加器 ：分布式共享只写变量


    //这里的累加不是纯粹意义上的增加，只要是数据的聚合都可以。加减都可以


    val acc=new MyAccumulator()
    //注册
    sc.register(acc,"numAvg")

    var sum= sc.longAccumulator("numAvg")//整数型
    rdd.foreach(
      num=>{
        acc.add(num)
      }
    )

println("numAvg="+acc.value)
    sc.stop()


  }

  //自定义累加器
  //继承AccumulatorV2，约束类型
  //Int:先累加器增加的数据类型
  //OUT:累加器的计算结果类型
  //2.重写方法，有6个

  class MyAccumulator extends  AccumulatorV2[Int,Int]{
    //定义临时的
      var total = 0
      var count = 0

    //判断当前累加器是否初始化状态
    override def isZero: Boolean = {
      total == 0 &&count==0
    }

    override def copy(): AccumulatorV2[Int, Int] = {
      new MyAccumulator
    }

    //重置
    override def reset(): Unit = {
      total =0
      count=0

    }
    // 累加器增加数据
    override def add(num: Int): Unit = {
      total+=num
      count+=1
    }
    //合并累加器的值
    //当前的累加器和其他累加器合并，运行的结果再和其他
    override def merge(other: AccumulatorV2[Int, Int]): Unit = {
      val acc: MyAccumulator = other.asInstanceOf[MyAccumulator]

      this.total+=acc.total
      this.count+=acc.count
    }
    //获取累加器的值（计算结果） 平均值
    override def value: Int = {
      total/ count
    }
  }

}
