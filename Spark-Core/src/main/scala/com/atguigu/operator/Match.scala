package com.atguigu.operator

object Match {

    def main(args: Array[String]): Unit = {


        // java中的switch有问题，所以scala没有这样的语法
        //     switch穿透现象
        /*
             switch( a ) => {
                 case 10:
                     sout("zzzzz")
                 case 20:
                     sout("xxxxx")
                     break;
                 default: println("yyyyy")
             }

         */
        // match的作用是为了代替于多重分支判断（switch）
        val a : String = "a"

        // 模式匹配规则是从上往下匹配。如果匹配上，执行箭头后面的内容，而且无需考虑跳出问题，因为会自动跳出
        // 如果一个都不匹配，那么会执行下划线的分支，这里的下划线表示任意值。
        // 如果此时没有下划线的分支，那么就意味着一个都匹配不上，会发生错误。MatchError
        a match {
            case "a" => println("aaaaa")
            case "b" => println("bbbbb")
            case _ => println("other")
        }

    }
}
