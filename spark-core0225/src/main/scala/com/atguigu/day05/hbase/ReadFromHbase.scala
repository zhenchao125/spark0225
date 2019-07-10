package com.atguigu.day05.hbase

import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ReadFromHbase {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val hbaseConf= HBaseConfiguration.create()
        hbaseConf.set("hbase.zookeeper.quorum", "hadoop201,hadoop202,hadoop203")
        hbaseConf.set(TableInputFormat.INPUT_TABLE, "student")
    
        val rdd: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
            hbaseConf,
            classOf[TableInputFormat],
            classOf[ImmutableBytesWritable],
            classOf[Result]
        )
        val rdd2 = rdd.map {
            case (ibw, result) => {
                
                val cells: Array[Cell] = result.rawCells()
                var name = ""
                var weigth = ""
                for (cell <- cells) {
                    if("name" == Bytes.toString(CellUtil.cloneQualifier(cell))){
                        name = Bytes.toString(CellUtil.cloneValue(cell))
                    }else if("weight" == Bytes.toString(CellUtil.cloneQualifier(cell))){
                        weigth = Bytes.toString(CellUtil.cloneValue(cell))
                    }
                }
                Student(Bytes.toString(result.getRow).toInt, name, weigth)
            }
        }
        rdd2.collect.foreach(println)
        sc.stop()
        
    }
}

/*
hbase和MapReduce的整合:
 

 */