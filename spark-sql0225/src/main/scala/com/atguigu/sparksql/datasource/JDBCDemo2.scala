package com.atguigu.sparksql.datasource

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object JDBCDemo2 {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("Test")
            .getOrCreate()
        import spark.implicits._
        val list1 = List(30, 50, 70, 60, 10, 20)
        val df: DataFrame = list1.toDF("id")
        
        // 通用的的写法:
        /*df.write.format("jdbc")
            .option("url", "jdbc:mysql://hadoop201:3306/rdd")
            .option("user", "root")
            .option("password", "aaa")
            .option("dbtable", "user")
            .mode(SaveMode.Append)
            .save()*/
        
        // 专用的写法
        val props = new Properties()
        props.setProperty("user", "root")
        props.setProperty("password", "aaa")
        df.write.mode("append")
                .jdbc("jdbc:mysql://hadoop201:3306/rdd", "user", props)
        
        spark.stop()
    }
}
