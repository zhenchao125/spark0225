package com.atguigu.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-07-08 16:41
  */
object Aggrage {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
//        val rdd1 = sc.parallelize(Array(30, 50, 70, 60, 10, 20))
//        val result: Int = rdd1.aggregate(0)(_ + _, _ + _)
//        val rdd1 = sc.parallelize(Array("a", "b", "c", "d"))
        // 零值使用次数: 分区数 + 1
//        val result: String = rdd1.aggregate("x")(_ + _, _ + _)  // xab   xcd
        
        
        val rdd1 = sc.parallelize(Array(0,0,0,0,0,0), 3)
//        val result: Int = rdd1.aggregate(1)(_ + _, _ + _)
        val result: Int = rdd1.fold(1)(_ + _)
        println(result)
        
        sc.stop()
        
        
        
    }
}
/*
RDD[(k,(k,(k, v)))]

aggregate(zero)(seq, opt)
    行动算子
    注意 零值在分区内使用, 分区间聚合的时候也会使用一次
    
fold(zero)(opt)
    如果aggregate的分区内聚合算法和分区间的聚合算法一致, 可以用fold来替换

 */
