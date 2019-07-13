package com.atguigu.saprkstreaming

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Window2 {
    def main(args: Array[String]): Unit = {
        // 1. 创建StreamingContext, 参数表示时间间隔
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DStreamHello")
        val ssc = new StreamingContext(conf, Seconds(4))
        ssc.checkpoint("./update")
        
        // 2. 创建 DStream
        val dStream = ssc.socketTextStream("hadoop201", 10000).window(Seconds(12), Seconds(4))
        
        //        dStream.flatMap(_.split("\\W+")).map((_, 1)).reduceByKey(_ + _).print
        //        dStream.flatMap(_.split("\\W+")).map((_, 1)).reduceByKey(_ + _).saveAsTextFiles("0225", "txt")
        dStream.flatMap(_.split("\\W+")).map((_, 1)).reduceByKey(_ + _).foreachRDD(rdd => {
            rdd.foreachPartition(it => {
                val sql = "insert into user values(?)"
                Class.forName("com.mysql.jdbc.Driver")
                val conn: Connection = DriverManager.getConnection("jdbc:mysql://hadoop201:3306/rdd", "root", "aaa")
                it.foreach(x => {
                    val ps: PreparedStatement = conn.prepareStatement(sql)
                    ps.setInt(1, x._2)
                    ps.executeUpdate()
                    ps.close()
                })
                conn.close()
            })
            
        })
        // 4. 启动 SparkStreaming
        ssc.start()
        
        // 5. 等待ssc停止
        ssc.awaitTermination()
        
    }
}
