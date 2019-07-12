package com.atguigu.sparksql.datasource

import java.util.Properties

import org.apache.spark.sql.SparkSession

object JDBCDemo1 {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("Test")
            .getOrCreate()
        
        // 专用的写法
        /*val props = new Properties()
        props.setProperty("user", "root")
        props.setProperty("password", "aaa")
        val df = spark.read.jdbc("jdbc:mysql://hadoop201:3306/rdd", "user", props)*/
    
        // 通用的写法
        val df = spark.read
            .format("jdbc")
            .option("url", "jdbc:mysql://hadoop201:3306/rdd")
            .option("user", "root")
            .option("password", "aaa")
            .option("dbtable", "user")
            .load()
    
        
        df.createOrReplaceTempView("user")
        spark.sql("select * from user where id > 20").show
        spark.stop()
    }
}
