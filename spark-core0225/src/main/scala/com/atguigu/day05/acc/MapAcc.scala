package com.atguigu.day05.acc

import org.apache.spark.util.AccumulatorV2

class MapAcc extends AccumulatorV2[Int, Map[String, Double]] {
    var map = Map[String, Double]()
    var count = 0 // 记录累加的元素的个数
    
    override def isZero: Boolean = map.isEmpty && count == 0
    
    override def copy(): AccumulatorV2[Int, Map[String, Double]] = {
        println("copy...")
        val acc = new MapAcc
        acc.map = Map[String, Double]() // 因为Map是不可变的, 所以需要重复赋值一个新的Map
        count = 0
        acc
    }
    
    override def reset(): Unit = {
        map = Map[String, Double]()
        count = 0
    }
    
    override def add(v: Int): Unit = { // 分期内在累加
        // sum max  min
        map += "sum" -> (map.getOrElse("sum", 0d) + v)
        map += "max" -> map.getOrElse("max", Long.MinValue.toDouble).max(v)
        map += "min" -> map.getOrElse("min", Long.MaxValue.toDouble).min(v)
        count += 1
    }
    
    // 分区间的合并
    override def merge(other: AccumulatorV2[Int, Map[String, Double]]): Unit = {
        other match {
            case o: MapAcc =>
                this.map += "sum" -> (map.getOrElse("sum", 0d) + o.map.getOrElse("sum", 0d))
                this.map += "max" -> map.getOrElse("max", Long.MinValue.toDouble).max(o.map.getOrElse("max", Long.MinValue.toDouble))
                this.map += "min" -> map.getOrElse("min", Long.MaxValue.toDouble).min(o.map.getOrElse("min", Long.MaxValue.toDouble))
                this.count += o.count
            
            case _ => throw new UnsupportedOperationException
        }
    }
    
    override def value: Map[String, Double] = {
        map += "avg" -> map.getOrElse("sum", 0d) / count
        map
    }
}
