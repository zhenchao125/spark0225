package com.atguigu.day04.partitioner

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MyPartitionerDemo1 {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(Array(30, 50, 70, 60, 10, 20))
        
        val rdd2: RDD[(Int, Int)] = rdd1.map((_, 1))
        // 1. partitionBy
        /*val rdd3: RDD[(Int, Int)] = rdd2.partitionBy(new MyPartitiner(2))
        rdd3.mapPartitionsWithIndex{
            case (index, it) => it.map((index, _))
        }.collect().foreach(println)*/
        
        //2. 执行一个shuffle算子
        rdd2.reduceByKey(new MyPartitiner(3), _ + _).mapPartitionsWithIndex{
            case (index, it) => it.map((index, _))
        }.collect().foreach(println)
        
        sc.stop()
        
    }
}
