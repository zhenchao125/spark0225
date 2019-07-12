package com.atguigu.saprkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * Author lzc
  * Date 2019-07-12 16:33
  */
object QueueRDDDstremDemo {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("QueueRDDDstremDemo")
        val ssc = new StreamingContext(conf, Seconds(5))
        
        val queue = mutable.Queue[RDD[Int]]()
        val source: InputDStream[Int] = ssc.queueStream(queue, false)
        
        source.print(100)
        
        ssc.start()
        while (true) {
            var rdd = ssc.sparkContext.parallelize(1 to 10)
            queue.enqueue(rdd)
            Thread.sleep(2000)
        }
        ssc.awaitTermination()
        
        
    }
}
