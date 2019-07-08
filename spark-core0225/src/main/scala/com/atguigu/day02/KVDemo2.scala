package com.atguigu.day02

import org.apache.spark.{SparkConf, SparkContext}

object KVDemo2 {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)
        
        //                val rdd2= rdd1.aggregateByKey(Int.MinValue)((last, x) => last.max(x) , _ + _)
        //        val rdd2= rdd1.aggregateByKey(0)(_ + _ , _ + _)
        
        //        val rdd2 = rdd1.combineByKey(x => x, (z: Int, ele: Int) => z.max(ele), (max1: Int, max2: Int) => max1 + max2)
        //        val rdd2 = rdd1.combineByKey(
        //            x => (x,x),
        //            (z: (Int, Int), ele: Int) => (z._1.max(ele), z._2.min(ele)),
        //            (mm1: (Int, Int), mm2: (Int, Int)) => (mm1._1 + mm2._1, mm1._2 + mm2._2))
        
        //        val rdd2 = rdd1.sortByKey()
        
        /*val rdd2 = rdd1.map{
            case (key, value) => (key, value + 100)
        }*/
        //        var rdd2 = rdd1.mapValues(v => v * v)
        
//        val rdd2 = rdd1.flatMapValues(v => Array(v, v * v, v * v * v))
//        rdd2.collect.foreach(println)
        
        sc.stop()
    }
}
