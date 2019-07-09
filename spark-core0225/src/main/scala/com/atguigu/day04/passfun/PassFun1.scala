package com.atguigu.day04.passfun

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-07-09 09:10
  */
object PassFun1 {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(Array(30, 50, 70, 60, 10, 20))
        
        val rdd2 = rdd1.map(x => x + 1)
        rdd2.collect.foreach(println)
        sc.stop()
        
    }
}
