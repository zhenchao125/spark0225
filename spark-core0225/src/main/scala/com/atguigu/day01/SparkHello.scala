package com.atguigu.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Author lzc
  * Date 2019-07-06 14:09
  */
object SparkHello {
    
    
    def main(args: Array[String]): Unit = {
        // spark程序
        
        // 1. 创建 SparkContext
        val conf: SparkConf = new SparkConf().setAppName("SparkHello")
        val sc = new SparkContext(conf)
        // 2. 通过 SparkContext得到 RDD
        val lines: RDD[String] = sc.textFile(args(0))
        // 3. 对RDD做各种转换操作
        val resultRDD: RDD[(String, Int)] = lines.flatMap(_.split("\\W+")).map((_, 1)).reduceByKey(_ + _)
        // 4. 对RDD做行动操作
        val result: Array[(String, Int)] = resultRDD.collect
        result.foreach(println)
        
        // 5. 停止SparkContext
        sc.stop()
    }
}
