package com.atguigu.day05.acc

import org.apache.spark.util.AccumulatorV2

class MyLongAcc extends AccumulatorV2[Long, Long] {
    // 用来记录累加的值
    var sum: Long = 0
    
    // 用来判断累加器是否为 "0"
    override def isZero: Boolean = sum == 0
    
    // 复制累加器
    override def copy(): AccumulatorV2[Long, Long] = {
        println("copy...")
        val acc = new MyLongAcc
        acc.sum = 0
        acc
    }
    
    // 重置累加器
    override def reset(): Unit = {
        sum = 0
    }
    
    // 真正的累加的功能
    override def add(v: Long): Unit = sum += v
    
    // 和并两个累加器  把other合并到this
    override def merge(other: AccumulatorV2[Long, Long]): Unit = {
        other match {
            case o: MyLongAcc => this.sum += o.sum
            case _ => throw new UnsupportedOperationException
        }
    }
    
    // 最终的返回值
    override def value: Long = sum
}
