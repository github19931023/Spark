package com.atguigu.operator

import scala.collection.immutable

object Match2 {

    def main(args: Array[String]): Unit = {

        // 匹配类型
        // Tuple, 元组（元素的组合）

        // 模式匹配(规则匹配)也可以简化

        // Tuple规则的匹配
        //val (_, name, _) = (1, "zhangsan", 30)

        //println(id)
        //println(name)
        //println(age)

        val list = Map("a"->1, "b"->2, "c"->3)

//        for ( ("b", v) <- map ) {
//            //println("key = " + k + ", value = " + v)
//            println("value = " + v)
//        }
        val strings: immutable.Iterable[String] =
            list.map {
                case ("b", v) => {
                    " value = " + v
                }
                case (k, v) => k + " = " + v
            }
        println(strings.mkString(","))

    }
}
