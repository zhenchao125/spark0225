package com.atguigu.sparksql.hive

import org.apache.spark.sql.SparkSession

object HiveDemo {
    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME", "atguigu")
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("Test")
            .enableHiveSupport()  // 使spark支持外置的hive
            .config("spark.sql.warehouse.dir", "hdfs://hadoop201:9000/user/hive/warehouse")
            .getOrCreate()
        import spark.implicits._
        
//        spark.sql("show databases").show
        spark.sql("create database spark3025").show()
        spark.stop()
        
        
    }
}
