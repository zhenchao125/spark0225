package com.atguigu.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object Sample {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(Array(30, 50, 70, 60, 10, 20, 30, 50, 70, 60, 10, 20))
        
//        val rdd2: RDD[Int] = rdd1.sample(false, 1)
//        println(rdd2.collect.toList)
    
//        println(rdd1.distinct().collect.toList)
        // 合并分区中的数据,  减少分区
        rdd1.coalesce(2)
        
        sc.stop()
        
    }
}
/*
sorted
sortWith
sortBy

 */