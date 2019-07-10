package com.atguigu.day05.acc

import org.apache.spark.{SparkConf, SparkContext}

object AccDemo3 {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(Array(30, 50, 70, 60, 10, 20), 2)
        // 计算这些值的和, 平均值, 最大值和最小值
        val acc = new MapAcc
        sc.register(acc, "mapAcc")
        rdd1.foreach(x => acc.add(x))
    
        println(acc.value)
        sc.stop()
    }
}
/*
累加器:
    对什么东西累加, 累加完成之后给别人什么类型的数据
    
    1. 元素累加
    2. "sum" -> 100   "avg" -> ... "max" -> ...

 */
