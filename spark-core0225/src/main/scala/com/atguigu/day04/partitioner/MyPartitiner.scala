package com.atguigu.day04.partitioner

import org.apache.spark.Partitioner

class MyPartitiner(val num: Int) extends Partitioner{
    override def numPartitions: Int = num
    
    override def getPartition(key: Any): Int = (key.hashCode() % num).abs
    
    
    override def equals(obj: scala.Any): Boolean = super.equals(obj)
    
    override def hashCode(): Int = super.hashCode()
}
