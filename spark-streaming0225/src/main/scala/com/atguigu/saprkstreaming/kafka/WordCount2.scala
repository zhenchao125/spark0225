package com.atguigu.saprkstreaming.kafka

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount2 {
    
    def createSSC() = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount1")
        val ssc = new StreamingContext(conf, Seconds(5))
        ssc.checkpoint("./ck-kafka")
        // kafka 参数
        //kafka参数声明
        val brokers = "hadoop201:9092,hadoop202:9092,hadoop203:9092"
        val group = "bigdata"
        val topic = "spark0225"
        val kafkaParams = Map(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
            ConsumerConfig.GROUP_ID_CONFIG -> group
        )
    
        val dstream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
            ssc,
            kafkaParams,
            Set(topic)
        )
    
        dstream.map(_._2).flatMap(_.split("\\W+")).map((_, 1)).reduceByKey(_ + _).print
        ssc
    }
    
    def main(args: Array[String]): Unit = {
        
        val ssc = StreamingContext.getActiveOrCreate("./ck-kafka", createSSC)
        
        ssc.start()
        ssc.awaitTermination()
        
    }
}

/*
1. kafka 配置

2. StreamingContext

3. 可以使用  KafkaUtils来创建 DStream
 */
