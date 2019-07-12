package com.atguigu.saprkstreaming

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.net.Socket

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

class MyReceiver(val host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {
    // 接收器启动的时候, 会自动回调这个函数
    override def onStart(): Unit = {
        // 启动一个子线程来接收数据
        new Thread() {
            override def run(): Unit = receive
        }.start()
    }
    
    // 接收数据:  从一个Socket中接收数据
    def receive = {
        try {
            val socket = new Socket(host, port)
            val in: InputStream = socket.getInputStream
            // 怎么读一行
            val reader = new BufferedReader(new InputStreamReader(in, "utf-8"))
            var line = reader.readLine()
            while (line != null) {
                // 把数据给executor
                store(line)
                line = reader.readLine()
            }
        } catch {
            // 重启
            case e =>
        } finally {
            restart("restart....")
        }
    }
    
    // 接收器关闭的时候会自动回调这个函数
    override def onStop(): Unit = {
    
    }
}
