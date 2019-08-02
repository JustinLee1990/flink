package com.lijuntao.flink

import java.security.Policy.Parameters

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 滑动窗口
 * 每隔一秒统计两秒内的数据，打印到控制台
 *
 */

object SocketWindowWordCountScala {
  def main(args: Array[String]): Unit = {
    //或缺socket端口号
    val port:Int = try {
    ParameterTool.fromArgs(args).getInt("port")
    }catch {
      case e: Exception =>{
        System.err.println("prot set err")
      }
        9001
    }

    //获取运行环境
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //获取数据源
    val text = env.socketTextStream("192.168.29.129",port,'\n')
    //必须添加因式转换，否则下面的flatMap方法会报错
    import org.apache.flink.api.scala._
    //解析数据、分组、窗口计算，聚合
    val windowCounts =
    text.flatMap(line => line.split("\\s"))//打平，把每一行切开
      .map(w =>WordWithCount(w,1))//把单词转成word,1形势
      .keyBy("word")//分组
      .timeWindow(Time.seconds(2),Time.seconds(1))//指定窗口大小，时间间隔
      .reduce((a,b) => WordWithCount(a.word,a.count+b.count))

    windowCounts.print().setParallelism(1);
    //执行任务
    env.execute("SocketWindowWordCountScala job")

  }

  case class WordWithCount(word:String,count:Long)
}
