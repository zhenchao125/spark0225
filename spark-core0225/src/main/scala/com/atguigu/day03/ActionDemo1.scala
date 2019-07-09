package com.atguigu.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-07-08 16:03
  */
object ActionDemo1 {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(Array(300, 50, 70, 60, 10, 20))
    
        /*println(rdd1.map(x => {
            println(x)
            x
        }).filter(x => {
            println("filter:" + x)
            x > 20
        }).reduce(_ + _))*/
        
//        rdd1.map((_, 1)).reduceByKey(_ + _).collect.foreach(println)
//        rdd1.map((_, 1)).countByKey.foreach(println)
    
//        println(rdd1.count)
        
//        rdd1.take(3).foreach(println)
        
//        rdd1.foreach(x => println(x))
        
        
        /*rdd1.foreach(x => {
            // 连接到mysql
            // 写
        })*/
        
       /* rdd1.foreachPartition(it => {
            // 建立到mysql的连接
        })
        */
        rdd1.takeOrdered(3).foreach(println)
        
        sc.stop()
    }
}

/*

转换算子
    共同特征: 他们仅仅用来构建 DAG(有向无环图)


行动算子
    collect
        把RDD中的数据, 拉取到driver端
        
    reduce
        直接对RDD中的是进行进行聚合
        
    countByKey
        统计不同的key的个数
        
        还是利用了reduceByKey
        
        和reduceByKey的区别
        
    count:
        统计rdd中元素的个数  Long
        
    take(n)
        取出rdd中的前n个
        先从0分区来取
        
    foreach
        遍历每个元素
        一般用于把元素转移到其他地方
        
    foreachPartition
        一个分区执行一次函数
        
    takeOrdered(n)
        取排序后的前n个, 只支持升序
        
    
        


 */
