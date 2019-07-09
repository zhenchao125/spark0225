package com.atguigu.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GroupBy {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(Array(30, 50, 7, 60, 1, 20))
        
        val rdd2: RDD[(Int, Iterable[Int])] = rdd1.groupBy(x => x % 2)
        rdd2.map{
            case (x, it) => (x, it.size)
        }.collect.foreach(println)
        sc.stop()
        
    }
}
