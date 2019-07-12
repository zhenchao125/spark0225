package com.atguigu.sparksql

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, StructField, StructType}

// select mySum(age) from user
class MySum extends UserDefinedAggregateFunction {
    
    // 声明输入的数据的数据类型  Double
    override def inputSchema: StructType = {
        StructType(StructField("inputColumn", DoubleType) :: Nil)
    }
    
    // 聚合的时候缓冲区的类型  Double
    override def bufferSchema: StructType = {
        StructType(StructField("sum", DoubleType) :: Nil)
    }
    
    // 最终输出的值的类型  Double
    override def dataType: DataType = DoubleType
    
    // 确定性:  相同的输入是否应用是有相同的输出
    override def deterministic: Boolean = true
    
    // 对缓冲区的初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        // 对和进行初始化  0的位置是存的和
        buffer(0) = 0d
    }
    
    
    // 同一个executor 内的聚合
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        // 传入的对应的位置的值不为null
        if (!input.isNullAt(0)) {
            // 从缓冲区取数据
            buffer(0) = buffer.getDouble(0) + input.getDouble(0)
        }
        
    }
    
    // 不同executor之间的聚合
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        if (!buffer2.isNullAt(0)) {
            buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
        }
    }
    
    // 最终的返回值
    override def evaluate(buffer: Row): Double = {
        buffer.getDouble(0)
    }
}
/*

 */