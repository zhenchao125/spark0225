package com.atguigu.sparksql

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Author lzc
  * Date 2019-07-12 09:08
  */
object RDD2DF {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
            .master("local[2]")
            .appName("RDD2DF")
            .getOrCreate()
        
        val list1 = List(("abc", 310), ("aaa", 130), ("ccc", 330))
        val rdd = spark.sparkContext.parallelize(list1).map {
            case (name, age) => Row(name, age)
        }
        
        //        val structType: StructType = StructType(Array(StructField("name", StringType),StructField("age", IntegerType)))
        val structType: StructType = StructType(StructField("name", StringType) :: StructField("age", IntegerType) :: Nil)
        val df: DataFrame = spark.createDataFrame(rdd, structType)
        
        df.show()
        spark.stop()
    }
}
