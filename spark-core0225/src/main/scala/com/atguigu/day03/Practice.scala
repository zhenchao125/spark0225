package com.atguigu.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-07-08 15:02
  */
object Practice {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        // 1. 先从文件读取数据
        val lines: RDD[String] = sc.textFile("c:/agent.log")
        // 2. map操作, 把里面的省份和广告取出来
        val proviceAdsOne: RDD[((String, String), Int)] = lines.map(line => {
            val split: Array[String] = line.split("\\W+")
            ((split(1), split(4)), 1)
        })
        // 3. 做聚合   4. 调整结构
        val proviceAdsCount = proviceAdsOne
            .reduceByKey(_ + _)
            .map {
                case ((pid, ads), count) => (pid, (ads, count))
            }
        
        // 5. 分组, 每组按照count进行倒序, 取前3
        val proviceGoupedAdsCount: RDD[(String, Iterable[(String, Int)])] = proviceAdsCount.groupByKey
        /*val proviceAdsCountTop3: RDD[(String, List[(String, Int)])] = proviceGoupedAdsCount.map {
            case (pid, adsCountIt) => {
                val top3 = adsCountIt.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
                (pid, top3)
            }
        } *//*.sortBy(_._1.toInt)*/
        val proviceAdsCountTop3: RDD[(String, List[(String, Int)])] = proviceGoupedAdsCount.mapValues(adsCountIt =>
            adsCountIt.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
        )
        proviceAdsCountTop3.collect.foreach(println)
        sc.stop()
    }
}

/*
1.	数据结构：时间戳，省份，城市，用户，广告，字段使用空格分割。
1516609143867 6 7 64 16
1516609143869 9 4 75 18
1516609143869 1 7 87 12
 	下载数据
	
2.	需求: 统计出每一个省份广告被点击次数的 TOP3

=> RDD[String]  map
=> RDD[((pid, aid), 1)]	 reduceByKey
=> RDD[((pid, aid), count)]	   map
=> RDD[(pid, (aid, count))]   groupByKey
RDD[(pid, Iterable[(aid, count)]]
 */