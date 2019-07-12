package com.atguigu.saprkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author lzc
  * Date 2019-07-12 15:50
  */
object DStreamHello {
    def main(args: Array[String]): Unit = {
        // 1. 创建StreamingContext, 参数表示时间间隔
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DStreamHello")
        val ssc = new StreamingContext(conf, Seconds(5))
        
        // 2. 创建 DStream
        val dStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop201", 10000)
        
        // 3. 对Dstream做操作:  转换和行动
        val wordCountDStream: DStream[(String, Int)] = dStream
            .flatMap(_.split("\\W+")).map((_, 1)).reduceByKey(_ + _)
        wordCountDStream.print
        
        // 4. 启动 SparkStreaming
        ssc.start()
        
        // 5. 等待ssc停止
        ssc.awaitTermination()
        
    }
}

/*

SparkContext
    RDD
SparkSession
 DF, DS

StreamingContext
    DStream


 */