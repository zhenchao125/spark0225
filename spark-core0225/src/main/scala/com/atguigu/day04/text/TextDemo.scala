package com.atguigu.day04.text

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TextDemo {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1: RDD[String] = sc.textFile("c:/input")
        rdd1.collect
        sc.stop()
        
    }
}
