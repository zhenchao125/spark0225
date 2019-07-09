package com.atguigu.day04.cache

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-07-09 10:57
  */
object RDDCachDemo1 {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(Array(30, 50, 20, 30, 50, 70, 60, 10, 20, 30, 50, 70, 60, 10, 20))
        
        var rdd2 = rdd1.map(x => {
            println("map:" + x)
            (x, 1)
        })
        // 将来第一次执行行动算子的时候, 会把rdd2的数据缓存到内存中
        rdd2.cache()
//        rdd2.persist(StorageLevel.MEMORY_ONLY)
        rdd2.collect
        println("----------------------------")
        rdd2.collect
        
        
        
        Thread.sleep(10000000)
        sc.stop()
        
        
    }
}
/*
rdd的缓存:

persist  cache  这种缓存机制, 一般都是使用内存.  他仍然记着他们的依赖关系
血缘关系还在.
 

 */