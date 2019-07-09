package com.atguigu.day04.mysql

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object MysqlDemo {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
       
        //定义连接mysql的参数
        val driver = "com.mysql.jdbc.Driver"
        val url = "jdbc:mysql://hadoop201:3306/rdd"
        val userName = "root"
        val passWd = "aaa"
        
        
        val rdd = new JdbcRDD[(Int, String)](
            sc,
            () => {
                Class.forName(driver)
                DriverManager.getConnection(url, userName, passWd)
            },
            "select id from user1 where ? <= id and id <= ?",
            10,
            100,
            3,
            result => (result.getInt(1), "abc")
        )
        
        rdd.collect.foreach(println)
        
        sc.stop()
        
    }
}
