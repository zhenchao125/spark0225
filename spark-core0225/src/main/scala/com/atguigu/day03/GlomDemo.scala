package com.atguigu.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GlomDemo {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("GlomDemo").setMaster("local[2]")
        
        val sc = new SparkContext(conf)
        val rdd1: RDD[Int] = sc.parallelize(1 to 10)
        val rdd2: RDD[Array[Int]] = rdd1.glom()
       /* rdd1.glom().map(arr => )
        rdd1.mapPartitions(it => it.toArray.)*/
        rdd2.collect.foreach(arr => println(arr.mkString(", ")))
        sc.stop()
    }
}
