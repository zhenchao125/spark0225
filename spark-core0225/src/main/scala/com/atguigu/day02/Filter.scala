package com.atguigu.day02

import org.apache.spark.{SparkConf, SparkContext}

object Filter {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(Array(30, 50, 70, 60, 10, 20))
        
        rdd1.filter(x => x > 20).collect.foreach(println)
        
        
        sc.stop()
        
    }
}
