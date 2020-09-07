package com.atguigu.operator

object Match1 {

    def main(args: Array[String]): Unit = {

        // 匹配类型
        //val a : Any = "zhangsan"
        //val a : Any = Array(1,2,3,4)
        //val a : Any = Array("a", "b", "c")
        val a : Any = List("a", "b", "c")
        //泛型擦除：编译的时候考虑泛型，执行的时候不考虑泛型，这就是泛型擦除

        // 模式匹配不考虑泛型的。

        // Scala中数组其实不是泛型

        // Array(1,2,3,4) => int[4] ints = {1,2,3,4}
        a match {
            case i:Int => println("int")
            case s:String => println("string")
            case a:Array[Int] => println("array[int]")
            case b:List[Int] => println("List[int]"+"  "+b)
            case _ => println("other")
        }

    }
}
