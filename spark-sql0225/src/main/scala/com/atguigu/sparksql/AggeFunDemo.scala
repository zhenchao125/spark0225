package com.atguigu.sparksql

import org.apache.spark.sql.{DataFrame, SparkSession}

object AggeFunDemo {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("Test")
            .getOrCreate()
        import spark.implicits._
    
        val list1 = List(("abc", 310), ("aaa", 130), ("ccc", 330), ("aaa", 130), ("ccc", 330), ("aaa", 130), ("ccc", 330))
        val df: DataFrame = list1.toDF("name", "age")
        df.createOrReplaceTempView("user")
        
        // 注册自定义的聚合函数
//        spark.udf.register("mySum", new MySum)
        spark.udf.register("my_remark", new MyRemark)
        spark.sql(
            """
              |select
              | name,
              | my_remark(age)
              |from user
              |group by name
            """.stripMargin).show(100)
        spark.stop()
        
    }
}
/*
"总和: ..., 平均值多少: ..., 数量: ...."

 */