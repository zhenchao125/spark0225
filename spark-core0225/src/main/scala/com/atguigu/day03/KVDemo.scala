package com.atguigu.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.util.Random

object KVDemo {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize((1 to 100).map(x => new Random().nextInt(20)))
//        val rdd1 = sc.parallelize(List(3, 50, 70, 60, 1, 20), 2)
        val rdd2: RDD[(Int, Int)] = rdd1.map((_, 1))
//        val rdd3: RDD[(Int, Int)] = rdd2.reduceByKey(_ + _)
        val rdd3: RDD[(Int, Iterable[Int])] = rdd2.groupByKey
        rdd3.map{
            case(k, it) => (k, it.sum)
        }.collect.foreach(println)
        
        // 分区器
//        val rdd3: RDD[(Int, Int)] = rdd2.partitionBy(new HashPartitioner(2))
//        rdd3.glom().map( x=> x.toList).collect.foreach(println)
        
        sc.stop()
        
        
    }
}
