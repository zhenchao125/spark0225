package com.atguigu.day04.cache

import org.apache.spark.{SparkConf, SparkContext}

object CheckpointDemo {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        // 设置 checkpoint的目录
        sc.setCheckpointDir("./ck1")
        val rdd1 = sc.parallelize(Array(30, 50, 20, 30, 50))
        
        val rdd2 = rdd1
            .filter(_ > 20)
            .map(x => {
                
                (x, System.currentTimeMillis())
            })
            .reduceByKey(_ + _)
        
        rdd2.checkpoint() // 指定一个要做checkpoint的计划
        // 第一次碰到一个行动算子, 先执行这个job. 这个job执行结束之后再重新启动一个新的job, 把这个job执行的结果checkpoint到目录内
        rdd2.cache()
        println(rdd2.collect.toList)
        println("----------------------------")
        println(rdd2.collect.toList)
        println(rdd2.collect.toList)
        println(rdd2.collect.toList)
        
        
        Thread.sleep(10000000)
        sc.stop()
    }
}

/*
checkpoint会切断他们的血缘关系


rdd的持久化:
    cache
        把rdd缓存到内存.  rdd的血缘关系仍然保留着
        一旦内存中的数据被清理, 会重新建立新的数据


    checkpoint
        需要把数据存储到磁盘上, 会切断rdd的血缘关系
        
        会重新启动一个job来做checkpoint
        
     
     两个会同时使用

 */
