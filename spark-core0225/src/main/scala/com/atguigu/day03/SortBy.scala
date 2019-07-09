package com.atguigu.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-07-08 16:59
  */
object SortBy {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(Array((User(10), 1), (User(20), 1)))
        
        /*val rdd1 = sc.parallelize(Array(30, 50, 70, 1))
        
        rdd1.map(x => {
            println(x)
            x
        }).sortBy(x => x)*/
        
        
        /*implicit val ord: Ordering[User] = new Ordering[User]{
            override def compare(x: User, y: User): Int = x.age - y.age
        }*/
        rdd1.sortByKey().collect.foreach(println)
        sc.stop()
    }
}

case class User(age: Int) extends Ordered[User] {
    override def compare(that: User): Int = this.age - that.age
}