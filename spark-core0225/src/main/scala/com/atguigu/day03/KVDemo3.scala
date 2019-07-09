package com.atguigu.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object KVDemo3 {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        var rdd1 = sc.parallelize(Array((1, "a"), (1, "b"), (2, "c"), (4, "d")))
        var rdd2 = sc.parallelize(Array((1, "aa"), (3, "bb"), (2, "cc"), (1, "xx")))
        
//        val rdd3: RDD[(Int, (String, String))] = rdd1.join(rdd2)
//        val rdd3: RDD[(Int, (String, Option[String]))] = rdd1.leftOuterJoin(rdd2)
//        val rdd3 = rdd1.rightOuterJoin(rdd2)
//        val rdd3 = rdd1.fullOuterJoin(rdd2)
        val rdd3 = rdd1.cogroup(rdd2)
        rdd3.collect.foreach(println)
        sc.stop()
        
    }
}
/*
内连接
左外连接
右外连接
全连接
 */
