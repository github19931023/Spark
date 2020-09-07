package com.atguigu.spark.rdd;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class Generic_Java {
    public static void main(String[] args) {

        // TODO 1. 泛型介绍
        //      JDK1.5 - 马丁 - Pizza
        //      类型和泛型？
        //   类型主要修饰对象（变量）类型，参数类型，方法返回值类型
        //   泛型主要修饰指定类型中内部数据的类型(约束) : 类型参数
        //
        Test<String> test = new Test<String>();
        //test.data // String
        Test test1 = new Test();
        //test1.data // Object

        // TODO 2. Java 中泛型不可变
        Test<User> test2 = new Test<User>();
        //Test<User> test3 = new Test<Parent>(); // （X）
        //Test<User> test4 = new Test<Child>();  // (X)

        // TODO 3. 类型擦除
        //  JVM没有泛型的概念，所以泛型其实在编译器层面起作用

        // TODO 4. 泛型的问题
        //     外部数据类型： List
        //     内部数据类型：String
        //     类型和泛型是截然不同的概念，不同当成整体来使用

        //     如果泛型相同，那么类型之间可以存在上下级关系。
        //List<String> stringList = new ArrayList<String>();
        //test(stringList);

        // TODO 5. 泛型的使用
        //      为了能让反省过使用起来更加方便，java提供了特殊的操作：泛型的上限和下限
        //      类树 ： 类的体系树
        //      上限： <? extends User>, 从指定的类型向类树的下层查找
        //             获取数据时，数据的功能不丢失
        //      下限：<? super User>, 从指定的类型向类树的上层查找
        //             生产数据时，不能产生功能更多的数据，没有意义
        Test<User> t1 = new Test<User>();
        // t1.m = new Message<Parent>(); //(X)
        //t1.m = new Message<User>();
        //t1.m = new Message<Child>();
        final Message<? extends User> consumer = t1.consumer();
        final User t = consumer.t;


        //t1.m1 = new Message<Parent>();
        //t1.m1 = new Message<User>();
        //t1.m1 = new Message<Child>(); // (X)

    }
    public static void test( List<Object> list ) {

    }
    public static void test( Collection<String> list ) {

    }
}
class Test<T> {
    public Message<? extends T> m;
    public Message<? super T> m1;
    // 消费数据
   public Message<? extends T> consumer() {
       return m;
   }
   public void produce( Message<? super T> m ) {

   }
}
class Message<T> {
    public T t;
}
class Parent {

}
class User extends Parent {

}
class Child extends User {

}
