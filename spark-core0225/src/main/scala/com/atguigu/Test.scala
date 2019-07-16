package com.atguigu

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-07-16 16:48
  */
object Test {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(Array(30, 50, 70)).map((_, "a"))
        val rdd2 = sc.parallelize(Array(30, 50, 70, 60, 10, 20)).map((_, "b"))
        
//        val rdd3: RDD[(Int, (String, String))] = rdd1.join(rdd2)
        
        val bc: Broadcast[Array[(Int, String)]] = sc.broadcast(rdd1.collect())
    
        val rdd3: RDD[(Int, (String, String))] = rdd2.map {
            case (k, v) => {
                val v2 = bc.value.toMap.get(k)
                (k, (v, v2))
            }
        }.filter{
            case (k, (v1, v2)) => v2 != None
        }.map{
            case (k, (v1, v2)) => (k, (v1, v2.get))
        }
        rdd3.collect.foreach(println)
        
        sc.stop()
        
    }
}
