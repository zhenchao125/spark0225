package com.atguigu.saprkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Window1 {
    def main(args: Array[String]): Unit = {
        // 1. 创建StreamingContext, 参数表示时间间隔
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DStreamHello")
        val ssc = new StreamingContext(conf, Seconds(4))
        ssc.checkpoint("./update")
        
        // 2. 创建 DStream
        val dStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop201", 10000)
        
        // 每隔4秒钟计算一下最近10秒的单词的个数
        /*val result: DStream[(String, Int)] = dStream.flatMap(_.split("\\W+")).map((_, 1))
            .reduceByKeyAndWindow(_ + _, Seconds(6))  // 默认步长是周期*/
       /* val result: DStream[(String, Int)] = dStream.flatMap(_.split("\\W+")).map((_, 1))
            .reduceByKeyAndWindow((_:Int) + (_:Int), Seconds(6), Seconds(4))*/
    
        val result: DStream[(String, Int)] = dStream.flatMap(_.split("\\W+")).map((_, 1))
            .reduceByKeyAndWindow((_:Int) + (_:Int), (x, y) => x - y ,Seconds(12), Seconds(4), filterFunc = (kv) => kv._2 > 0)
        result.print
        // 4. 启动 SparkStreaming
        ssc.start()
        
        // 5. 等待ssc停止
        ssc.awaitTermination()
        
    }
}
