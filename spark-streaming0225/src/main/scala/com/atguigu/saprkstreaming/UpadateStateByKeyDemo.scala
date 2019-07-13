package com.atguigu.saprkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object UpadateStateByKeyDemo {
    def main(args: Array[String]): Unit = {
        // 1. 创建StreamingContext, 参数表示时间间隔
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DStreamHello")
        val ssc = new StreamingContext(conf, Seconds(5))
        ssc.checkpoint("./update")
        
        // 2. 创建 DStream
        val dStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop201", 10000)
        
        /*dStream.flatMap(_.split("\\W+")).map((_, 1)).updateStateByKey((seq: Seq[Int], opt: Option[Int]) =>
            Some(seq.sum + opt.getOrElse(0))
        ).print*/
    
        val result: DStream[(String, Int)] = dStream.flatMap(_.split("\\W+")).map((_, 1)).updateStateByKey {
            case (seq, opt) => Some(seq.sum + opt.getOrElse(0))
        }
        result.print
        // 4. 启动 SparkStreaming
        ssc.start()
        
        // 5. 等待ssc停止
        ssc.awaitTermination()
        
    }
}
