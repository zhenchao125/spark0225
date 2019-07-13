package com.atguigu.saprkstreaming.kafka

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaCluster.Err
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object WordCount3 {
    
    val brokers = "hadoop201:9092,hadoop202:9092,hadoop203:9092"
    val group = "bigdata"
    val topic = "spark0225"
    val kafkaParams = Map(
        "zookeeper.connect" -> "hadoop201:2181,hadoop202:2181,hadoop203:2181",
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
        ConsumerConfig.GROUP_ID_CONFIG -> group
    )
    // 用来处理offsets的KafkaCluster
    val kafkaCluster = new KafkaCluster(kafkaParams)
    
    
    // 读取上次消费到的offsets
    def readOffsets(): Map[TopicAndPartition, Long] = {
        var result = Map[TopicAndPartition, Long]()
        // 1. 先获取所有的分区
        val topicAndPartitionSetEither: Either[Err, Set[TopicAndPartition]] = kafkaCluster.getPartitions(Set(topic))
        topicAndPartitionSetEither match {
            // 获取到topic和分区的情况
            case Right(topicAndPartitions) =>
                // 获取到每个topic和每个分区的ofset的信息
                val topicAndPartitionOffsetMapEither: Either[Err, Map[TopicAndPartition, Long]] = kafkaCluster.getConsumerOffsets(group, topicAndPartitions)
                if (topicAndPartitionOffsetMapEither.isRight) {
                    result ++= topicAndPartitionOffsetMapEither.right.get
                } else { //有topic和分区, 但是没有相关的偏移量
                    // 把每个topic的每个分区的偏移都设置为0
                    topicAndPartitions.foreach(topicAndPartion => {
                        result += topicAndPartion -> 0
                        //                        result += ((topicAndPartion, 0))
                    })
                }
            
            case _ =>
        }
        
        result
    }
    
    // 保存消费的offsets
    def saveOffsets(dstream: InputDStream[String]) = {
        // 应该每隔5秒钟更新一次offsets
        dstream.foreachRDD(rdd => {
            val map = mutable.Map[TopicAndPartition, Long]()
            val hasOffsetRanges: HasOffsetRanges = rdd.asInstanceOf[HasOffsetRanges]
            val ranges: Array[OffsetRange] = hasOffsetRanges.offsetRanges
            
            ranges.foreach(offsetRange => {
                val topicAndPartition = offsetRange.topicAndPartition()
                val offset: Long = offsetRange.untilOffset
                
                map += topicAndPartition -> offset
            })
            
            kafkaCluster.setConsumerOffsets(group, map.toMap)
        })
    }
    
    def main(args: Array[String]): Unit = {
        
        val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount1")
        val ssc = new StreamingContext(conf, Seconds(5))
        // 读取offset
        val offsets: Map[TopicAndPartition, Long] = readOffsets()
        // 得到DStream
        val dstream: InputDStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](
            ssc,
            kafkaParams,
            offsets,
            (messageHandler: MessageAndMetadata[String, String]) => messageHandler.message()
        
        )
        dstream.print
        // 保存offset
        saveOffsets(dstream)
        ssc.start()
        ssc.awaitTermination()
        
    }
}

/*
1. kafka 配置

2. StreamingContext

3. 可以使用  KafkaUtils来创建 DStream
 */
