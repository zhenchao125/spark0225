package com.atguigu.day05.acc

import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object AccDemo1 {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(Array(30, 50, 70, 60, 10, 20), 4)
        
        val acc: LongAccumulator = sc.longAccumulator
        
        rdd1.foreach(x => {
            acc.add(1)
        })
        println("------------------------")
        println("a: " + acc.value)
        sc.stop()
    }
}
/*

 */
