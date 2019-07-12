package com.atguigu.sparksql

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/*
select
 my_remark(age)
from user

"总和: ..., 平均值多少: ..., 数量: ...."
*/

class MyRemark extends UserDefinedAggregateFunction {
    override def inputSchema: StructType = StructType(StructField("inputColumn", DoubleType) :: Nil)
    
    override def bufferSchema: StructType = {
        StructType(StructField("sum", DoubleType) :: StructField("count", LongType) :: Nil)
    }
    
    override def dataType: DataType = StringType
    
    override def deterministic: Boolean = true
    // 初始化 和 和count
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        // 缓存sum
        buffer(0) = 0d
        // 缓存count
        buffer(1) = 0L
    }
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        if(!input.isNullAt(0)){
            buffer(0) = buffer.getDouble(0) + input.getDouble(0)
            buffer(1) = buffer.getLong(1) + 1L
        }
    }
    
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        if(!buffer2.isNullAt(0)){
            buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
            buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
        }
    }
    // "总和: ..., 平均值多少: ..., 数量: ...."
    override def evaluate(buffer: Row): String = {
        s"总和: ${buffer.getDouble(0)}, 平均值: ${buffer.getDouble(0) / buffer.getLong(1)}, 数量: ${buffer.getLong(1)}"
    }
}
