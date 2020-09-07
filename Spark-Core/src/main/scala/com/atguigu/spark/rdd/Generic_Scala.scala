package com.atguigu.spark.rdd

object Generic_Scala {

    def main(args: Array[String]): Unit = {

        // TODO 1. 泛型不可变
//        val t1 : Test[User] = new Test[User]
//        val t2 : Test[User] = new Test[Parent] // (X)
//        val t3 : Test[User] = new Test[Child]  // (X)

        // TODO 2. Scala中泛型的变化 : 协变 & 逆变
        // 默认情况下，类型和泛型是两个不同的概念，所以使用起来容易出现错误。
        // Scala为了让我们使用起来更加的方便，进行改善
        // 将类型和泛型当成一个整体来使用，进行访问
        // TODO 协变 ： class Test[+T]
        //val t1 : Test[User] = new Test[User]
       // val t2 : Test[User] = new Test[Parent] //(X)
        //val t3 : Test[User] = new Test[Child]

        // TODO 逆变： class Test[-T]
        //val t1 : Test[User] = new Test[User]
        //val t2 : Test[User] = new Test[Parent]
        //val t3 : Test[User] = new Test[Child] // (X)

        // TODO 为了和Java兼容，Scala也提供了泛型的上限和下限的使用。但是采用的是颜文字
        //  泛型的上限 ：java => extends, scala => 【 <: 】
        //  泛型的下限 ：java => super, scala => 【 >: 】
        //val t1 : Test[User] = new Test[User]
        //val t: User = t1.comsume.t

        // TODO 看懂Scala中的泛型
    /*    val users: List[User] = List( new User(), new User() )
        println(users.reduce[Child](
            (x, y) => x
        ))

*/
    }
    class Test[T] {

        def comsume : Message[_ <: T] = {
            null
        }
        def produce( m : Message[ _ >: T] ): Unit = {

        }
    }
    class Message[T] {
        var t : T = _
    }
}
