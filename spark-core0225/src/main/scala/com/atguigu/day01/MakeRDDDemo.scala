package com.atguigu.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-07-06 16:11
  */
object MakeRDDDemo {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("MakeRDDDemo").setMaster("local[2]")
        val sc = new SparkContext(conf)
        
        val rdd1: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7), 3)
        //        val rdd2: RDD[Int] = rdd1.map(x => x * x)
        //        var rdd2 = rdd1.mapPartitions(it => it.map(x => x * x))
//        val rdd2 = rdd1.mapPartitionsWithIndex {
//            case (index, it) => it.map((index, _))
//        }
        val rdd2: RDD[Int] = rdd1.flatMap(x => Array(x, x * x, x * x * x))
        rdd2.collect.foreach(println)
        sc.stop()
        
    }
}
