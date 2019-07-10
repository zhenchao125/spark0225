package com.atguigu.day05.broadcast

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

import javax.servlet.http.HttpServletRequest
import javax.ws.rs.core.Application
/**
  * Author lzc
  * Date 2019-07-10 11:16
  */
object BCDemo {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(Array(30, 50, 70, 60, 10, 20), 4)
    
        // 实际开发的时候, 很多时候都需要把一些数据广播出去
        val bc: Broadcast[Range.Inclusive] = sc.broadcast(1 to 1000000)
//
        rdd1.foreach(x => println(bc.value.contains(x)))
        
        Thread.sleep(100000)
        sc.stop()
    }
}
