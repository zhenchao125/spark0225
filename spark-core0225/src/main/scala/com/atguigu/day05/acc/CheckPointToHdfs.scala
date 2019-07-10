package com.atguigu.day05.acc

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-07-10 08:13
  */
object CheckPointToHdfs {
    def main(args: Array[String]): Unit = {
        
        System.setProperty("HADOOP_USER_NAME", "atguigu")
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        sc.setCheckpointDir("hdfs://hadoop201:9000/ck0225")
        val rdd1 = sc.parallelize(Array(30, 50, 70, 60, 10, 20))
        
        rdd1.checkpoint()
        rdd1.collect
        sc.stop()
        
    }
    
    
}
