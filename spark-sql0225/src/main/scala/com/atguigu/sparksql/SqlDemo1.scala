package com.atguigu.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author lzc
  * Date 2019-07-10 16:47
  */
object SqlDemo1 {
    def main(args: Array[String]): Unit = {
        // 1. 先有SparkSession
        val spark: SparkSession = SparkSession.builder()
            .appName("SqlDemo1")
            .master("local[2]")
            .getOrCreate()
        import spark.implicits._
        // 2. 得到df或ds
        val rdd: RDD[(String, Int)] =
            spark.sparkContext.parallelize(List(("lisi", 20),("ww", 30),("zs", 40)))
        val df: DataFrame = rdd.toDF("name", "age")
        // 3. 创建临时表
        df.createOrReplaceTempView("user")
        // 4. 执行sql
        spark.sql(
            """
              |select
              | *
              |from user
            """.stripMargin).show()
        // 5. 关闭
        spark.stop()
    }
}
/*

 */