package com.atguigu.day05.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-07-10 09:31
  */
object WriteToHbase {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        
        val hbaseConf = HBaseConfiguration.create()
        hbaseConf.set("hbase.zookeeper.quorum", "hadoop201,hadoop202,hadoop203")
        hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, "student")
        
        // 通过job来设置输出的格式的类
        val job = Job.getInstance(hbaseConf)
        job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
        job.setOutputKeyClass(classOf[ImmutableBytesWritable])
        job.setOutputValueClass(classOf[Put])
        
        val initialRDD = sc.parallelize(List(("100", "apple", "110"), ("200", "banana", "120"), ("300", "pear", "130")))
        val hbaseRDD = initialRDD.map(x => {
            val put = new Put(Bytes.toBytes(x._1))
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(x._2))
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("weight"), Bytes.toBytes(x._3))
            (new ImmutableBytesWritable(), put)
        })
        hbaseRDD.saveAsNewAPIHadoopDataset(job.getConfiguration)
    }
    
}
